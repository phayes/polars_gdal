mod unprocessed_series;

use gdal::vector::LayerAccess;
use gdal::Dataset;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use unprocessed_series::*;

pub fn file_to_df<P: AsRef<Path>>(path: P) -> Result<DataFrame, ()> {
    let dataset = Dataset::open(path).unwrap();

    let mut layer = dataset.layer(0).unwrap();
    let feat_count = layer.try_feature_count();

    let mut unprocessed_series_map = HashMap::new();

    for feature in layer.features() {
        for (idx, (name, value)) in feature.fields().enumerate() {
            if idx == 0 && name == "geometry" {
                panic!("Field named 'geometry' is not allowed");
            }
            let entry = unprocessed_series_map
                .entry(name.clone())
                .or_insert_with(|| UnprocessedSeries {
                    name: name.clone(),
                    nullable: false,
                    datatype: gdal_type_to_unprocessed_type(&value),
                    data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
                });

            if value.is_none() && !entry.nullable {
                entry.nullable = true;
            }

            entry.data.push(GdalData::Value(value));
        }

        let entry = unprocessed_series_map
            .entry("geometry".to_owned())
            .or_insert_with(|| UnprocessedSeries {
                name: "geometry".to_owned(),
                nullable: false,
                datatype: UnprocessedDataType::Geometry,
                data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
            });

        let wkb = feature.geometry().wkb().unwrap();
        entry.data.push(GdalData::Geometry(wkb));
    }

    // Process the HashMap into a Vec of Series
    let mut series_vec = Vec::with_capacity(unprocessed_series_map.len());
    for (_, unprocessed_series) in unprocessed_series_map {
        if let UnprocessedDataType::Null = unprocessed_series.datatype {
            continue;
        }
        series_vec.push(unprocessed_series.process());
    }

    Ok(DataFrame::new(series_vec).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data() {
        let _df = file_to_df("test_data/us_states.feature_collection.implicit_4326.json").unwrap();
        //println!("{}", _df);

        let _df = file_to_df("test_data/global_large_lakes.feature_collection.implicit_4326.json").unwrap();
        //println!("{}", _df); 
    }
}
