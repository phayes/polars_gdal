#![doc = include_str!("../README.md")]

mod error;
mod unprocessed_series;

#[cfg(test)]
mod test;

pub use error::*;
pub extern crate gdal;

use gdal::errors::GdalError;
use gdal::spatial_ref::SpatialRef;
use gdal::vector::FieldValue as GdalValue;
use gdal::vector::LayerAccess;
use gdal::vector::OGRFieldType;
use gdal::Dataset;
use gdal::LayerOptions;
use polars::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use unprocessed_series::*;

/// Parameters to configure the conversion of a GDAL dataset to a Polars DataFrame.
#[derive(Debug, Default)]
pub struct ReadParams<'a> {
    /// GDal bitflags used by [`Dataset::open_ex`]. Flags are combined with a bitwise OR `|`.
    ///
    /// # Example
    /// ```
    /// use polars_gdal::gdal;
    ///
    /// let mut params = polars_gdal::ReadParams::default();
    /// params.open_flags = gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_VERBOSE_ERROR;
    /// ```
    pub open_flags: gdal::GdalOpenFlags,

    /// List of allowed GDAL drivers. See <https://gdal.org/drivers/vector/index.html>
    pub allowed_drivers: Option<&'a [&'a str]>,

    /// Array of "KEY=value" strings to pass to the GDAL driver. See <https://gdal.org/drivers/vector/index.html>
    ///
    /// # Example
    /// ```
    /// use polars_gdal::gdal;
    ///
    /// let mut params = polars_gdal::ReadParams::default();
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

    /// The Feature ID column name. By default, the feature-id column is not included.
    pub fid_column_name: Option<&'a str>,

    /// The Geometry colum name. By default `geomery` will be used.
    ///
    /// Changing this is not recommended since the `geopolars` crates assumes geometries will be stored in the `geometry` column.
    pub geometry_column_name: Option<&'a str>,

    /// The Geometry format to use, defaults to WKB. In the future, this will default to GeoArrow format.
    pub geometry_format: GeometryFormat,

    /// Stop reading after this many features. If None, all features will be read.
    pub truncating_limit: Option<usize>,

    /// The maximum number of features to read. If this limit is surpassed, an error will be returned.
    pub erroring_limit: Option<usize>,

    /// Start reading features at this offset.
    pub offset: Option<usize>,
}

/// Parameters to configure the conversion of a Polars DataFrame to a GDAL dataset.
#[derive(Debug, Default)]
pub struct WriteParams<'a> {
    /// For multi-layer files, the specific layer to read. If None, the first layer will be read.
    pub layer_name: Option<&'a str>,

    /// The Geometry colum name. By default `geomery` will be used.
    pub geometry_column_name: Option<&'a str>,

    /// The Geometry format to use, defaults to WKB. In the future, this will default to GeoArrow format.
    pub geometry_format: GeometryFormat,

    /// The Feature ID column name.
    pub fid_column_name: Option<&'a str>,

    /// The SRS of the newly created layer, or `None` for no SRS.
    pub srs: Option<&'a SpatialRef>,

    /// The type of geometry for the new layer, or `None` to auto-detect the geometry type.
    pub geometry_type: Option<gdal::vector::OGRwkbGeometryType::Type>,

    /// Additional driver-specific options to pass to GDAL, in the form `name=value`.
    pub options: Option<&'a [&'a str]>,
}

impl<'a> Into<gdal::DatasetOptions<'a>> for &ReadParams<'a> {
    fn into(self) -> gdal::DatasetOptions<'a> {
        gdal::DatasetOptions {
            open_flags: self.open_flags,
            allowed_drivers: self.allowed_drivers,
            open_options: self.open_options,
            sibling_files: self.sibling_files,
        }
    }
}

/// The geometry format to use when reading or writing to the dataframe.
///
/// Defaults to WKB, in the future this default will change to GeoArrow format
#[derive(Debug, Clone, Copy)]
pub enum GeometryFormat {
    /// Write the geometry as WKB (Well Known Binary) format.
    WKB,

    /// Write the geometry as GeoJSON format.
    GeoJson,

    /// Write the geometry as GeoJSON format.
    WKT,
}

impl Default for GeometryFormat {
    fn default() -> Self {
        Self::WKB
    }
}

impl Into<UnprocessedDataType> for GeometryFormat {
    fn into(self) -> UnprocessedDataType {
        match self {
            Self::WKB => UnprocessedDataType::GeometryWKB,
            Self::GeoJson => UnprocessedDataType::String,
            Self::WKT => UnprocessedDataType::String,
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
/// use polars_gdal::df_from_bytes;
///
/// let geojson = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"foo"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"bar"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#.as_bytes();
/// let df = df_from_bytes(geojson, None).unwrap();
/// println!("{}", df);
/// ```
pub fn df_from_bytes(
    data: &[u8],
    filename_hint: Option<&str>,
    params: Option<ReadParams>,
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
    if params.open_flags & gdal::GdalOpenFlags::GDAL_OF_READONLY
        != gdal::GdalOpenFlags::GDAL_OF_READONLY
    {
        return Err(Error::ReadonlyMustSet);
    }
    if params.open_flags & gdal::GdalOpenFlags::GDAL_OF_UPDATE
        == gdal::GdalOpenFlags::GDAL_OF_UPDATE
    {
        return Err(Error::UpdateNotSupported);
    }

    // Generate a safe path to the data that is exclusive to this process-id and uses the filename hint
    static DF_FROM_BYTS_MEM_FILE_INCREMENTOR: AtomicU64 = AtomicU64::new(0);
    let input_mem_path = format!(
        "/vsimem/polars_gdal/df_from_bytes/{}/{}/{}",
        std::process::id(),
        DF_FROM_BYTS_MEM_FILE_INCREMENTOR.fetch_add(1, Ordering::SeqCst),
        filename_hint
    );

    // Call into the C function VSIFileFromMemBuffer
    // SAFETY: VSIFileFromMemBuffer accepts a pointer to mutable data because in other circumstances it can be used to write data.
    //         However, we're ensuring that it's only opened in read-only mode, which allows us to safely coerce a immutable &[u8] to a *mut u8.
    let path = CString::new(input_mem_path.as_bytes()).unwrap();
    let ptr = data.as_ptr() as *mut u8;
    let handle =
        unsafe { VSIFileFromMemBuffer(path.as_ptr(), ptr, data.len() as u64, true as i32) };
    if handle.is_null() {
        return Err(_last_null_pointer_err("VSIGetMemFileBuffer").into());
    }

    // Load the dataset and layer from the VSI file handler
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
/// use polars_gdal::df_from_resource;
/// let df = df_from_resource("my_shapefile.shp", None).unwrap();
/// println!("{}", df);
/// ```
///
/// # Remote file example
/// ``` # ignore
/// use polars_gdal::df_from_resource;
/// let df = df_from_resource("https://raw.githubusercontent.com/ebrelsford/geojson-examples/master/queens.geojson", None).unwrap();
/// println!("{}", df);
/// ```
///
/// # PostGIS example
/// ``` # ignore
/// use polars_gdal::{df_from_resource, Params};
///
/// let mut params = crate::Params::default();
/// params.layer_name = Some("some_table_name");
/// let df = df_from_resource("postgresql://user:pass@hostname/dbname", Some(params)).unwrap();
/// println!("{}", df);
/// ```
pub fn df_from_resource<P: AsRef<Path>>(
    path: P,
    params: Option<ReadParams>,
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
/// use polars_gdal::{df_from_layer, gdal};
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
    params: Option<ReadParams>,
) -> Result<DataFrame, Error> {
    let feat_count = layer.try_feature_count();

    let params = params.unwrap_or_default();
    let fid_column_name = params.fid_column_name;
    let geometry_column_name = params.geometry_column_name.unwrap_or("geometry");
    let geometry_format = params.geometry_format;

    let mut numkeys = 0;

    let mut field_series_map = HashMap::new();
    let mut geom_series = UnprocessedSeries {
        name: geometry_column_name.to_owned(),
        nullable: false,
        datatype: geometry_format.into(),
        data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
    };

    let mut fid_series = UnprocessedSeries {
        name: fid_column_name.unwrap_or("").to_owned(),
        nullable: false,
        datatype: UnprocessedDataType::Fid,
        data: Vec::with_capacity(feat_count.unwrap_or(100) as usize),
    };

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
        if fid_column_name.is_some() {
            if let Some(fid) = feature.fid() {
                fid_series.data.push(GdalData::Fid(fid));
            }
        }

        // Process Geometry
        let geometry = feature.geometry();
        if geometry.is_empty() {
            geom_series.data.push(GdalData::Value(None));
        } else {
            match geometry_format {
                GeometryFormat::WKB => {
                    let wkb = geometry.wkb()?;
                    geom_series.data.push(GdalData::Geometry(wkb));
                }
                GeometryFormat::WKT => {
                    let wkt = geometry.wkt()?;
                    geom_series
                        .data
                        .push(GdalData::Value(Some(GdalValue::StringValue(wkt))));
                }
                GeometryFormat::GeoJson => {
                    let geojson = geometry.json()?;
                    geom_series
                        .data
                        .push(GdalData::Value(Some(GdalValue::StringValue(geojson))));
                }
            }
        }

        // Process all data fields
        let mut field_count = 0;
        for (name, value) in feature.fields() {
            let entry = field_series_map.entry(name.clone()).or_insert_with(|| {
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
            for entry in field_series_map.values_mut() {
                if entry.data.len() < idx + 1 {
                    entry.data.push(GdalData::Value(None));

                    if !entry.nullable {
                        entry.nullable = true;
                    }
                }
            }
        }
    }

    // If there's naming conflicts, rename conflicting fields
    if let Some(mut conflicting_series) = field_series_map.remove(geometry_column_name) {
        conflicting_series.name = format!("{}_original", geometry_column_name);
        field_series_map.insert(conflicting_series.name.clone(), conflicting_series);
    }
    if let Some(fid_column_name) = fid_column_name {
        if let Some(mut conflicting_series) = field_series_map.remove(fid_column_name) {
            conflicting_series.name = format!("{}_original", fid_column_name);
            field_series_map.insert(conflicting_series.name.clone(), conflicting_series);
        }
    }

    // Process the HashMap into a Vec of Series
    let mut series_vec = Vec::with_capacity(field_series_map.len() + 2);

    // Process the Feature ID first
    if fid_column_name.is_some() {
        series_vec.push(fid_series.process());
    }

    // Process the field series
    for (_, unprocessed_series) in field_series_map {
        if let UnprocessedDataType::Null = unprocessed_series.datatype {
            continue;
        }
        series_vec.push(unprocessed_series.process());
    }

    // Process the geometry series
    series_vec.push(geom_series.process());

    Ok(DataFrame::new(series_vec)?)
}

/// Given a dataframe, create a GDAL layer
///
/// Given a pre-existing GDAL Dataset, create a new layer from a Polars dataframe.
///
/// # Example
/// ```rust # ignore
/// let df: DataFrame = ...;
/// let json_driver = gdal::DriverManager::get_driver_by_name("GeoJSON")?;
/// let mut dataset: gldal::Dataset = json_driver.create_vector_only("my_json_file.json")?;
/// layer_from_df(&df, &mut dataset)?;
/// dataset.flush_cache();
/// ```
pub fn gdal_layer_from_df<'a>(
    df: &DataFrame,
    dataset: &'a mut gdal::Dataset,
    params: Option<WriteParams>,
) -> Result<gdal::vector::Layer<'a>, Error> {
    let params = params.unwrap_or_default();

    let geometry_column_name = params.geometry_column_name.unwrap_or("geometry");
    let row_count = df.height();

    if row_count == 0 {
        return Err(Error::EmptyDataframe);
    }

    // All prop columns as (col-index, name, field-type)
    let props: Vec<(usize, &str, OGRFieldType::Type)> = df
        .get_columns()
        .iter()
        .enumerate()
        .map(|(i, c)| (i, c.name(), polars_type_id_to_gdal_type_id(c.dtype())))
        .filter(|(_i, n, t)| *n != geometry_column_name && t.is_some())
        .map(|(i, n, t)| (i, n, t.unwrap()))
        .collect::<Vec<_>>();

    let geom_idx = df
        .find_idx_by_name(geometry_column_name)
        .ok_or_else(|| Error::CannotFindGeometryColumn(geometry_column_name.to_owned()))?;

    let mut row = df.get_row(0);

    let geom_type = match params.geometry_type {
        Some(geom_type) => geom_type,
        None => {
            let first_geom = polars_anyvalue_to_gdal_geometry(
                &row.0[geom_idx],
                params.geometry_format,
                geometry_column_name,
            )
            .map_err(|e| Error::UnableToDetermineGeometryType(format!("{}", e)))?;
            first_geom.geometry_type()
        }
    };

    let mut layer = dataset.create_layer(LayerOptions {
        name: geometry_column_name,
        srs: params.srs,
        ty: geom_type,
        options: params.options,
    })?;

    let fields_def: Vec<(&str, OGRFieldType::Type)> =
        { props.iter().map(|(_, n, t)| (*n, *t)).collect() };
    layer.create_defn_fields(&fields_def)?;

    for idx in 0..row_count {
        df.get_row_amortized(idx, &mut row);
        let geom = polars_anyvalue_to_gdal_geometry(
            &row.0[geom_idx],
            params.geometry_format,
            geometry_column_name,
        )?;
        let mut field_values = Vec::with_capacity(props.len());
        let mut field_names = Vec::with_capacity(props.len());
        for (i, n, _) in props.iter() {
            let val = polars_value_to_gdal_value(&row.0[*i]);
            if let Some(val) = val {
                field_values.push(val);
                field_names.push(*n);
            }
        }
        layer.create_feature_fields(geom, &field_names, &field_values)?
    }

    Ok(layer)
}

/// Given a dataframe, get bytes in a GDAL geospatial format
///
/// Currently, only vector drivers are supported. For raster support, use `gdal_layer_from_df`.
///
/// # Example
/// ```rust # ignore
/// let df: DataFrame = ...;
/// let json_driver = gdal::DriverManager::get_driver_by_name("GeoJSON")?;
/// let geojson_bytes = gdal_bytes_from_df(&df, &json_driver, None)?;
/// println!("{}", String::from_utf8(geojson_bytes)?);
/// ```
pub fn gdal_bytes_from_df(
    df: &DataFrame,
    driver: &gdal::Driver,
    params: Option<WriteParams>,
) -> Result<Vec<u8>, Error> {
    // Generate a safe path to the data that is exclusive to this process-id and uses the filename hint
    static BYTES_FROM_DF_MEM_FILE_INCREMENTOR: AtomicU64 = AtomicU64::new(0);
    let input_mem_path = format!(
        "/vsimem/polars_gdal/bytes_from_df/{}/{}/layer",
        std::process::id(),
        BYTES_FROM_DF_MEM_FILE_INCREMENTOR.fetch_add(1, Ordering::SeqCst),
    );

    // TODO: Support rasters
    let mut dataset = driver.create_vector_only(&input_mem_path)?;

    let _layer = gdal_layer_from_df(df, &mut dataset, params)?;
    dataset.flush_cache();

    let mut owned_bytes = vec![];
    gdal::vsi::call_on_mem_file_bytes(&input_mem_path, |bytes| {
        owned_bytes.extend_from_slice(bytes)
    })?;

    Ok(owned_bytes)
}

/// Given a dataframe, write to a GDAL resource path and return the dataset.
///
/// If given a path to local disk, the file will be written to local disk.
/// If given a URI for a GDAL supported remote resource, the dataframe will be written to that resource in the specified geospatial format.
///
/// Currently, only vector drivers are supported. For raster support, use `gdal_layer_from_df`.
///
/// # Example
/// ```rust # ignore
/// use polars_gdal::{gdal, gdal_resource_from_df};
///
/// let df: DataFrame = ...;
/// let shapefule_driver = gdal::DriverManager::get_driver_by_name("ESRI Shapefile")?;
/// let dataset = gdal_resource_from_df(&df, &shapefule_driver, "/some/path/my_shapefile.shp", None)?;
/// println!("{}", String::from_utf8(geojson_bytes)?);
/// ```
pub fn gdal_resource_from_df<P: AsRef<Path>>(
    df: &DataFrame,
    driver: &gdal::Driver,
    path: P,
    params: Option<WriteParams>,
) -> Result<Dataset, Error> {
    // TODO: Support rasters
    let mut dataset = driver.create_vector_only(path)?;

    let _layer = gdal_layer_from_df(df, &mut dataset, params)?;
    dataset.flush_cache();

    Ok(dataset)
}

fn polars_value_to_gdal_value(
    polars_val: &polars::datatypes::AnyValue,
) -> Option<gdal::vector::FieldValue> {
    match polars_val {
        AnyValue::Int8(val) => Some(GdalValue::IntegerValue(*val as i32)),
        AnyValue::Int16(val) => Some(GdalValue::IntegerValue(*val as i32)),
        AnyValue::Int32(val) => Some(GdalValue::IntegerValue(*val)),
        AnyValue::Int64(val) => Some(GdalValue::Integer64Value(*val)),
        AnyValue::UInt8(val) => Some(GdalValue::IntegerValue(*val as i32)),
        AnyValue::UInt16(val) => Some(GdalValue::IntegerValue(*val as i32)),
        AnyValue::UInt32(val) => Some(GdalValue::IntegerValue(*val as i32)),
        AnyValue::UInt64(val) => Some(GdalValue::Integer64Value(*val as i64)),
        AnyValue::Float32(val) => Some(GdalValue::RealValue(*val as f64)),
        AnyValue::Float64(val) => Some(GdalValue::RealValue(*val)),
        AnyValue::Utf8(val) => Some(GdalValue::StringValue(val.to_string())),
        AnyValue::Utf8Owned(val) => Some(GdalValue::StringValue(val.to_string())),
        AnyValue::Boolean(val) => Some(GdalValue::IntegerValue(*val as i32)),
        AnyValue::Date(_val) => todo!(),
        AnyValue::Time(val) => Some(GdalValue::Integer64Value(*val)),
        AnyValue::Datetime(_val, _unit, _opts) => todo!(),
        AnyValue::Duration(val, _) => Some(GdalValue::Integer64Value(*val)),
        AnyValue::List(_) => todo!(),
        AnyValue::Null => None,
        AnyValue::Binary(_) => None,
        AnyValue::BinaryOwned(_) => None,
    }
}

fn polars_type_id_to_gdal_type_id(polars_type: &DataType) -> Option<OGRFieldType::Type> {
    match polars_type {
        DataType::Int8 => Some(OGRFieldType::OFTInteger),
        DataType::Int16 => Some(OGRFieldType::OFTInteger),
        DataType::Int32 => Some(OGRFieldType::OFTInteger),
        DataType::Int64 => Some(OGRFieldType::OFTInteger64),
        DataType::UInt8 => Some(OGRFieldType::OFTInteger),
        DataType::UInt16 => Some(OGRFieldType::OFTInteger),
        DataType::UInt32 => Some(OGRFieldType::OFTInteger),
        DataType::UInt64 => Some(OGRFieldType::OFTInteger64),
        DataType::Float32 => Some(OGRFieldType::OFTReal),
        DataType::Float64 => Some(OGRFieldType::OFTReal),
        DataType::Utf8 => Some(OGRFieldType::OFTString),
        DataType::Boolean => Some(OGRFieldType::OFTInteger),
        DataType::Date => Some(OGRFieldType::OFTDate),
        DataType::Time => Some(OGRFieldType::OFTInteger64),
        DataType::Datetime(_, _) => Some(OGRFieldType::OFTDateTime),
        DataType::Duration(_) => Some(OGRFieldType::OFTInteger64),
        DataType::Binary => Some(OGRFieldType::OFTBinary),
        DataType::List(dtype) => match dtype.as_ref() {
            DataType::Int8 => Some(OGRFieldType::OFTIntegerList),
            DataType::Int16 => Some(OGRFieldType::OFTIntegerList),
            DataType::Int32 => Some(OGRFieldType::OFTIntegerList),
            DataType::Int64 => Some(OGRFieldType::OFTInteger64List),
            DataType::UInt8 => Some(OGRFieldType::OFTIntegerList),
            DataType::UInt16 => Some(OGRFieldType::OFTIntegerList),
            DataType::UInt32 => Some(OGRFieldType::OFTIntegerList),
            DataType::UInt64 => Some(OGRFieldType::OFTInteger64List),
            DataType::Utf8 => Some(OGRFieldType::OFTStringList),
            _ => None,
        },
        _ => None,
    }
}

fn polars_anyvalue_to_gdal_geometry(
    anyval: &AnyValue,
    geometry_format: GeometryFormat,
    geom_col: &str,
) -> Result<gdal::vector::Geometry, Error> {
    match geometry_format {
        GeometryFormat::WKB => match anyval {
            AnyValue::Binary(geom) => Ok(gdal::vector::Geometry::from_wkb(geom)?),
            _ => {
                Err(Error::GeometryColumnWrongType(
                    geom_col.to_owned(),
                    polars::datatypes::DataType::Binary,
                    anyval.dtype(),
                ))
            }
        },
        GeometryFormat::WKT => match anyval {
            AnyValue::Utf8(geom) => Ok(gdal::vector::Geometry::from_wkt(geom)?),
            AnyValue::Utf8Owned(geom) => Ok(gdal::vector::Geometry::from_wkt(geom.as_str())?),
            _ => {
                Err(Error::GeometryColumnWrongType(
                    geom_col.to_owned(),
                    polars::datatypes::DataType::Utf8,
                    anyval.dtype(),
                ))
            }
        },
        GeometryFormat::GeoJson => {
            todo!("TODO: Support GeoJSON via use of geozero");
        }
    }
}
