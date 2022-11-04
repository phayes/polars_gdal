mod unprocessed_series;
mod error;

pub use error::*;

use gdal::vector::LayerAccess;
use gdal::Dataset;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use unprocessed_series::*;

#[derive(Debug, Default)]
pub struct Params<'a> {
    pub open_flags: gdal::GdalOpenFlags,
    pub allowed_drivers: Option<&'a [&'a str]>,
    pub open_options: Option<&'a [&'a str]>,
    pub sibling_files: Option<&'a [&'a str]>,
    pub layer_name: Option<&'a str>,
    pub layer_index: Option<usize>,
    pub fid_column_name: Option<&'a str>,
    pub geometry_column_name: Option<&'a str>,
    pub truncating_limit: Option<usize>,
    pub erroring_limit: Option<usize>,
    pub offset: Option<usize>,
}

impl<'a> Into<gdal::DatasetOptions<'a>> for &Params<'a> {
    fn into(self) -> gdal::DatasetOptions<'a> {
        let mut options = gdal::DatasetOptions::default();
        options.open_flags = self.open_flags;
        options.allowed_drivers = self.allowed_drivers;
        options.open_options = self.open_options;
        options.sibling_files = self.sibling_files;
        options
    }
}

pub fn file_to_df<'a, P: AsRef<Path>>(
    path: P,
    params: Option<Params<'a>>,
) -> Result<DataFrame, Error> {
    let params = params.unwrap_or_default();
    let gdal_options: gdal::DatasetOptions = (&params).into();

    let dataset = Dataset::open_ex(path, gdal_options)?;

    let layer = if let Some(layer_name) = params.layer_name {
        dataset.layer_by_name(layer_name)?
    } else if let Some(layer_index) = params.layer_index {
        dataset.layer(layer_index as isize)?
    } else {
        dataset.layer(0)?
    };

    layer_to_df(layer, Some(params))
}

pub fn bytes_to_df<'a>(bytes: Vec<u8>, params: Option<Params<'a>>) -> Result<DataFrame, Error> {
    static MEM_FILE_INCREMENTOR: AtomicU64 = AtomicU64::new(0);

    let params = params.unwrap_or_default();
    let gdal_options: gdal::DatasetOptions = (&params).into();

    let input_mem_path = format!(
        "/vsimem/geopolars_gdal/{}/{}",
        std::process::id(),
        MEM_FILE_INCREMENTOR.fetch_add(1, Ordering::SeqCst)
    );
    gdal::vsi::create_mem_file(&input_mem_path, bytes)?;

    let dataset = gdal::Dataset::open_ex(&input_mem_path, gdal_options)?;
    
    let layer = if let Some(layer_name) = params.layer_name {
        dataset.layer_by_name(layer_name)?
    } else if let Some(layer_index) = params.layer_index {
        dataset.layer(layer_index as isize)?
    } else {
        dataset.layer(0)?
    };

    layer_to_df(layer, Some(params))
}

pub fn layer_to_df<'a>(
    layer: gdal::vector::Layer,
    params: Option<Params<'a>>,
) -> Result<DataFrame, Error> {
    let feat_count = layer.try_feature_count();

    let params = params.unwrap_or_default();
    let fid_column_name = params.fid_column_name.unwrap_or("fid");
    let geometry_column_name = params.geometry_column_name.unwrap_or("geometry");

    let mut unprocessed_series_map = HashMap::new();

    let mut layer = layer;
    for (idx, feature) in layer.features().enumerate() {
        if let Some(offset) = params.offset {
            if idx < offset {
                continue;
            }
        }
        if let Some(limit) = params.truncating_limit {
            if idx >= limit {
                break;
            }
        }
        if let Some(limit) = params.erroring_limit {
            if idx >= limit {
                return Err(Error::FeatureLimitReached(limit));
            }
        }

        if let Some(fid) = feature.fid() {
            let entry = unprocessed_series_map
                .entry(fid_column_name.to_owned())
                .or_insert_with(|| UnprocessedSeries {
                    name: fid_column_name.to_owned(),
                    nullable: false,
                    datatype: UnprocessedDataType::Fid,
                    data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
                });
            entry.data.push(GdalData::Fid(fid));
        }

        for (idx, (name, value)) in feature.fields().enumerate() {
            if idx == 0 && name == geometry_column_name {
                return Err(Error::GeometryColumnCollision(geometry_column_name.to_string()))
            }
            if idx == 0 && name == fid_column_name {
                return Err(Error::FidColumnCollision(fid_column_name.to_string()))
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
            .entry(geometry_column_name.to_owned())
            .or_insert_with(|| UnprocessedSeries {
                name: geometry_column_name.to_owned(),
                nullable: false,
                datatype: UnprocessedDataType::Geometry,
                data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
            });

        let wkb = feature.geometry().wkb()?;
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

    Ok(DataFrame::new(series_vec)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data() {
        let _df = file_to_df(
            "test_data/us_states.feature_collection.implicit_4326.json",
            None,
        )
        .unwrap();
        //println!("{}", _df);

        let _df = file_to_df(
            "test_data/global_large_lakes.feature_collection.implicit_4326.json",
            None,
        )
        .unwrap();
        //println!("{}", _df);
    }
}
