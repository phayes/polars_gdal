#![doc = include_str!("../README.md")]

mod error;
mod unprocessed_series;

pub use error::*;
pub extern crate gdal;

use gdal::errors::GdalError;
use gdal::vector::LayerAccess;
use gdal::Dataset;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use unprocessed_series::*;

/// Parameters to configure the conversion of a vector dataset to a Polars DataFrame.
#[derive(Debug, Default)]
pub struct Params<'a> {
    /// GDal bitflags used by [`Dataset::open_ex`]. Flags are combined with a bitwise OR `|`.
    ///
    /// # Example
    /// ```
    /// use geopolars_gdal::gdal;
    ///
    /// let mut params = geopolars_gdal::Params::default();
    /// params.open_flags = gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_VERBOSE_ERROR;
    /// ```
    pub open_flags: gdal::GdalOpenFlags,

    /// List of allowed GDAL drivers. See https://gdal.org/drivers/vector/index.html
    pub allowed_drivers: Option<&'a [&'a str]>,

    /// Array of "KEY=value" strings to pass to the GDAL driver. See https://gdal.org/drivers/vector/index.html
    ///
    /// # Example
    /// ```
    /// use geopolars_gdal::gdal;
    ///
    /// let mut params = geopolars_gdal::Params::default();
    /// let csv_parsing_options = ["EMPTY_STRING_AS_NULL=YES", "KEEP_GEOM_COLUMNS=NO", "X_POSSIBLE_NAMES=Lon*", "Y_POSSIBLE_NAMES=Lat*"];
    /// params.open_options = Some(&csv_parsing_options);
    /// ```
    pub open_options: Option<&'a [&'a str]>,

    /// Array of strings that are filenames that are auxiliary to the main filename (eg .dbf .proj and .shx files are auxiliary to .shp files).
    ///
    /// If left as None, a probing of the file system will be done.
    pub sibling_files: Option<&'a [&'a str]>,

    /// For multi-layer files, the specific layer to read. If None, the first layer will be read.
    pub layer_name: Option<&'a str>,

    /// For muti-layer files, the specific muti-layer to read. If None, the first layer will be read.
    ///
    /// This has no effect is `layer_name` is set.
    pub layer_index: Option<usize>,

    /// The Feature ID column name. By default `fid` will be used. A empty string can be set to disable reading the feature id.
    pub fid_column_name: Option<&'a str>,

    /// The Geometry colum name. By default `geomery` will be used.
    ///
    /// Changing this is not recommended since the `geopolars` crates assumes geometries will be stored in the `geometry` column.
    pub geometry_column_name: Option<&'a str>,

    /// Stop reading after this many features. If None, all features will be read.
    pub truncating_limit: Option<usize>,

    /// The maximum number of features to read. If this limit is surpassed, an error will be returned.
    pub erroring_limit: Option<usize>,

    /// Start reading features at this offset.
    pub offset: Option<usize>,
}

impl<'a> Into<gdal::DatasetOptions<'a>> for &Params<'a> {
    fn into(self) -> gdal::DatasetOptions<'a> {
        gdal::DatasetOptions {
            open_flags: self.open_flags,
            allowed_drivers: self.allowed_drivers,
            open_options: self.open_options,
            sibling_files: self.sibling_files,
        }
    }
}

/// Given some raw bytes, create a dataframe.
///
/// Formats supported include GeoJSON, Shapefile, GPKG, and others.
/// See [https://gdal.org/drivers/vector/index.html](https://gdal.org/drivers/vector/index.html) for a full list of supported formats.
/// Some formats require additional libraries to be installed.
///
/// Adding a filename hint can be very helpful in allowing GDAL to properly parse the datastream.
/// For example, zipped shapefiles can't be parsed without a filename hint in the form of "filename.shp.zip".
///
/// # Example
/// ``` # ignore
/// use geopolars_gdal::df_from_bytes;
///
/// let geojson = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"foo"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"bar"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#.as_bytes();
/// let df = df_from_bytes(geojson, None).unwrap();
/// println!("{}", df);
/// ```
pub fn df_from_bytes(
    data: &[u8],
    filename_hint: Option<&str>,
    params: Option<Params>,
) -> Result<DataFrame, Error> {
    use gdal_sys::VSIFCloseL;
    use gdal_sys::VSIFileFromMemBuffer;
    use std::ffi::c_char;
    use std::ffi::CStr;
    use std::ffi::CString;

    fn _last_null_pointer_err(method_name: &'static str) -> GdalError {
        let last_err_msg = _string(unsafe { gdal_sys::CPLGetLastErrorMsg() });
        unsafe { gdal_sys::CPLErrorReset() };
        GdalError::NullPointer {
            method_name,
            msg: last_err_msg,
        }
    }

    fn _string(raw_ptr: *const c_char) -> String {
        let c_str = unsafe { CStr::from_ptr(raw_ptr) };
        c_str.to_string_lossy().into_owned()
    }

    // Parse params and get defaults
    let params = params.unwrap_or_default();
    let gdal_options: gdal::DatasetOptions = (&params).into();
    let filename_hint = filename_hint.unwrap_or("layer");

    // Do some safety checks that are requied for the safety of the following unsafe parts
    if data.is_empty() {
        return Err(Error::EmptyData);
    }
    if params.open_flags & gdal::GdalOpenFlags::GDAL_OF_READONLY != gdal::GdalOpenFlags::GDAL_OF_READONLY {
        return Err(Error::ReadonlyMustSet);
    }
    if params.open_flags & gdal::GdalOpenFlags::GDAL_OF_UPDATE == gdal::GdalOpenFlags::GDAL_OF_UPDATE {
        return Err(Error::UpdateNotSupported);
    }

    // Generate a safe path to the data that is exclusive to this process-id and uses the filename hint
    static MEM_FILE_INCREMENTOR: AtomicU64 = AtomicU64::new(0);
    let input_mem_path = format!(
        "/vsimem/geopolars_gdal/{}/{}/{}",
        std::process::id(),
        MEM_FILE_INCREMENTOR.fetch_add(1, Ordering::SeqCst),
        filename_hint
    );
    
    // Call into the C function VSIFileFromMemBuffer
    // SAFETY: VSIFileFromMemBuffer accepts a pointed to mutable data because in other circumstances it can be used to write data. 
    //         However, we're ensuring that it's only opened in read-only mode, which allows us to safely coerse a immutable &[u8] to a *mut u8.
    let path = CString::new(input_mem_path.as_bytes()).unwrap();
    let ptr = data.as_ptr() as *mut u8;
    let handle =
        unsafe { VSIFileFromMemBuffer(path.as_ptr(), ptr, data.len() as u64, true as i32) };
    if handle.is_null() {
        return Err(_last_null_pointer_err("VSIGetMemFileBuffer").into());
    }

    // Load the dataset from the VSI file handler
    let dataset = gdal::Dataset::open_ex(&input_mem_path, gdal_options)?;
    let mut layer = if let Some(layer_name) = params.layer_name {
        dataset.layer_by_name(layer_name)?
    } else if let Some(layer_index) = params.layer_index {
        dataset.layer(layer_index as isize)?
    } else {
        dataset.layer(0)?
    };

    // Read the dataframe out of the layer
    let df = df_from_layer(&mut layer, Some(params));

    // Release the VSI handle
    unsafe {
        VSIFCloseL(handle);
    }

    // Return the dataframe
    df
}

/// Given a filepath or a URI, read the resource into a dataframe.
///
/// The simplest resource is a file on the local filesystem, in which case we would simply pass in a filepath.
/// Fetching resources over http(s) is supported using a URL.
/// Connecting to PostGIS is supported using a `postgres://user:pass@host/dbname` URI in combination with setting `Params::layer_name` to the name of the table.
///
/// Formats supported include GeoJSON, Shapefile, SpatialLite database, KML, and others.
/// See [https://gdal.org/drivers/vector/index.html](https://gdal.org/drivers/vector/index.html) for a full list of supported formats.
/// Some formats require additional libraries to be installed.
///
/// # Local file example
/// ``` # ignore
/// use geopolars_gdal::df_from_resource;
/// let df = df_from_resource("my_shapefile.shp", None).unwrap();
/// println!("{}", df);
/// ```
///
/// # Remote file example
/// ``` # ignore
/// use geopolars_gdal::df_from_resource;
/// let df = df_from_resource("https://raw.githubusercontent.com/ebrelsford/geojson-examples/master/queens.geojson", None).unwrap();
/// println!("{}", df);
/// ```
///
/// # PostGIS example
/// ``` # ignore
/// use geopolars_gdal::{df_from_resource, Params};
///
/// let mut params = crate::Params::default();
/// params.layer_name = Some("some_table_name");
/// let df = df_from_resource("postgresql://user:pass@hostname/dbname", Some(params)).unwrap();
/// println!("{}", df);
/// ```
pub fn df_from_resource<P: AsRef<Path>>(
    path: P,
    params: Option<Params>,
) -> Result<DataFrame, Error> {
    let params = params.unwrap_or_default();
    let gdal_options: gdal::DatasetOptions = (&params).into();

    let dataset = Dataset::open_ex(path, gdal_options)?;

    let mut layer = if let Some(layer_name) = params.layer_name {
        dataset.layer_by_name(layer_name)?
    } else if let Some(layer_index) = params.layer_index {
        dataset.layer(layer_index as isize)?
    } else {
        dataset.layer(0)?
    };

    df_from_layer(&mut layer, Some(params))
}

/// Given a GDAL layer, create a dataframe.
///
/// This can be used to manually open a GDAL Dataset, and then create a dataframe from a specific layer.
/// This is most useful when you want to preprocess the Dataset in some way before creating a dataframe,
/// for example by applying a SQL filter or a spatial filter.
///
/// # Example
/// ```rust # ignore
/// use geopolars_gdal::{df_from_layer, gdal};
/// use gdal::vector::sql;
///
/// let dataset = gdal::Dataset::open("my_shapefile.shp")?;
/// let query = "SELECT kind, is_bridge, highway FROM my_shapefile WHERE highway = 'pedestrian'";
/// let mut result_set = dataset.execute_sql(query, None, sql::Dialect::DEFAULT).unwrap().unwrap();
///
/// let df = df_from_layer(result_set.deref_mut(), None).unwrap();
/// println!("{}", df);
/// ```
pub fn df_from_layer<'l>(
    layer: &mut gdal::vector::Layer<'l>,
    params: Option<Params>,
) -> Result<DataFrame, Error> {
    let feat_count = layer.try_feature_count();

    let params = params.unwrap_or_default();
    let fid_column_name = params.fid_column_name.unwrap_or("fid");
    let geometry_column_name = params.geometry_column_name.unwrap_or("geometry");

    let mut numkeys = 0;

    let mut unprocessed_series_map = HashMap::new();

    for (idx, feature) in &mut layer.features().enumerate() {
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

        // Process FID
        if !fid_column_name.is_empty() {
            if let Some(fid) = feature.fid() {
                let fid_entry = unprocessed_series_map
                    .entry(fid_column_name.to_owned())
                    .or_insert_with(|| UnprocessedSeries {
                        name: fid_column_name.to_owned(),
                        nullable: false,
                        datatype: UnprocessedDataType::Fid,
                        data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
                    });
                fid_entry.data.push(GdalData::Fid(fid));
            }
        }

        // Process Geometry
        let geom_entry = unprocessed_series_map
            .entry(geometry_column_name.to_owned())
            .or_insert_with(|| UnprocessedSeries {
                name: geometry_column_name.to_owned(),
                nullable: false,
                datatype: UnprocessedDataType::Geometry,
                data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
            });

        let geometry = feature.geometry();
        if geometry.is_empty() {
            geom_entry.data.push(GdalData::Value(None));
        } else {
            let wkb = feature.geometry().wkb()?;
            geom_entry.data.push(GdalData::Geometry(wkb));
        }

        // Process all data fields
        let mut field_count = 0;
        for (name, value) in feature.fields() {
            if name == geometry_column_name {
                return Err(Error::GeometryColumnCollision(
                    geometry_column_name.to_string(),
                ));
            }
            if name == fid_column_name {
                return Err(Error::FidColumnCollision(fid_column_name.to_string()));
            }

            let entry = unprocessed_series_map
                .entry(name.clone())
                .or_insert_with(|| {
                    let mut series = UnprocessedSeries {
                        name: name.clone(),
                        nullable: false,
                        datatype: gdal_type_to_unprocessed_type(&value),
                        data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
                    };

                    // Fill data with nulls for past features
                    if idx != 0 {
                        for _ in 0..idx {
                            series.data.push(GdalData::Value(None));
                        }
                        series.nullable = true;
                    }
                    numkeys += 1;
                    series
                });

            if value.is_none() && !entry.nullable {
                entry.nullable = true;
            }

            entry.data.push(GdalData::Value(value));
            field_count += 1;
        }

        // If field_count doesn't match numkeys, top up any missing fields with nulls
        if field_count != numkeys {
            for entry in unprocessed_series_map.values_mut() {
                if entry.data.len() < idx + 1 {
                    entry.data.push(GdalData::Value(None));

                    if !entry.nullable {
                        entry.nullable = true;
                    }
                }
            }
        }
    }

    // Process the HashMap into a Vec of Series
    let mut series_vec = Vec::with_capacity(unprocessed_series_map.len());

    // Process the Feature ID first
    if !fid_column_name.is_empty() {
        if let Some(fid_series) = unprocessed_series_map.remove(fid_column_name) {
            series_vec.push(fid_series.process());
        }
    }

    // Save the geometry column for last
    let geometry_series = unprocessed_series_map.remove(geometry_column_name);

    for (_, unprocessed_series) in unprocessed_series_map {
        if let UnprocessedDataType::Null = unprocessed_series.datatype {
            continue;
        }
        series_vec.push(unprocessed_series.process());
    }

    // Add the geometry column last
    if let Some(geometry_series) = geometry_series {
        series_vec.push(geometry_series.process());
    }

    Ok(DataFrame::new(series_vec)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_df_from_resource() {
        // Test GeoJSON
        let _df = df_from_resource(
            "test_data/us_states.feature_collection.implicit_4326.json",
            None,
        )
        .unwrap();
        //println!("{}", _df);

        // Test GeoJSON
        let _df = df_from_resource(
            "test_data/global_large_lakes.feature_collection.implicit_4326.json",
            None,
        )
        .unwrap();
        //println!("{}", _df);

        // Test Shapefile
        let _df = df_from_resource("test_data/stations.shp", None).unwrap();
        // println!("{}", _df);

        // Test CSV with options
        let mut params = crate::Params::default();
        let csv_parsing_options = [
            "EMPTY_STRING_AS_NULL=YES",
            "KEEP_GEOM_COLUMNS=NO",
            "X_POSSIBLE_NAMES=Lon*",
            "Y_POSSIBLE_NAMES=Lat*",
        ];
        params.open_options = Some(&csv_parsing_options);
        let _df = df_from_resource("test_data/lat_lon_countries.csv", Some(params)).unwrap();
        // println!("{}", _df);

        // Test SpatialLite
        let _df = df_from_resource("test_data/test_spatialite.sqlite", None).unwrap();
        // println!("{}", _df);
    }

    #[test]
    fn test_df_from_bytes() {
        let geojson = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"foo"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"bar"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#.as_bytes();
        let _df = df_from_bytes(geojson, None, None).unwrap();
        //println!("{}", _df);

        let shapefile = include_bytes!("../test_data/stations_shapefile.shp.zip");
        let _df = df_from_bytes(shapefile, Some("stations_shapefile.shp.zip"), None).unwrap();
        //println!("{}", _df);
    }

    #[test]
    fn test_df_from_layer() {
        let geojson = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"foo"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"bar"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#.as_bytes().to_vec();

        let input_mem_path = format!("/vsimem/geopolars_gdal/test_geojson/layer");
        gdal::vsi::create_mem_file(&input_mem_path, geojson).unwrap();
        let dataset = gdal::Dataset::open(&input_mem_path).unwrap();

        let query = "SELECT * FROM layer WHERE name = 'foo'";
        let mut result_set = dataset
            .execute_sql(query, None, gdal::vector::sql::Dialect::DEFAULT)
            .unwrap()
            .unwrap();

        let _df = df_from_layer(&mut result_set, None).unwrap();
        //println!("{}", _df);
    }

    #[allow(dead_code)]
    fn test_postgis() {
        let mut params = crate::Params::default();
        params.layer_name = Some("parcel_polygon");
        params.truncating_limit = Some(100);

        let df = df_from_resource(
            "postgresql://postgres:postgres@localhost/carbon",
            Some(params),
        )
        .unwrap();
        println!("{}", df);
    }

    #[allow(dead_code)]
    fn test_https() {
        let df = df_from_resource(
            "https://raw.githubusercontent.com/ebrelsford/geojson-examples/master/queens.geojson",
            None,
        )
        .unwrap();
        println!("{}", df);
    }
}
