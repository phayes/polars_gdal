
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

    let input_mem_path = format!("/vsimem/polars_gdal/test_geojson/layer");
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

#[test]
fn test_pure_gdal() {
    use crate::gdal;
    let dataset = gdal::Dataset::open("test_data/stations_shapefile.shp.zip").unwrap();

    let json_driver = gdal::DriverManager::get_driver_by_name("GeoJson").unwrap();

    gdal::vsi::create_mem_file("/vsimem/polars_gdal/test_geojson/layer/", vec![]).unwrap();

    let mut json_dataset = dataset
        .create_copy(
            &json_driver,
            "/vsimem/polars_gdal/test_geojson/layer/test_geojson.json",
            &[],
        )
        .unwrap();
    json_dataset.flush_cache();

    let mut json_bytes = vec![];
    gdal::vsi::call_on_mem_file_bytes("/vsimem/polars_gdal/test_geojson/layer/test_geojson.json", |bytes| {
        json_bytes.extend_from_slice(bytes);
    }).unwrap();

    // Print JSON bytes as a string
    // println!("{}", String::from_utf8(json_bytes).unwrap());
}

#[test]
fn test_gdal_layer_from_df() {
    use std::io::Cursor;
    use polars::prelude::IpcReader;

    let df_bytes = include_bytes!("../test_data/cities.arrow");
    let cursor = Cursor::new(df_bytes);

    let df = IpcReader::new(cursor).finish().unwrap();

    let json_driver = gdal::DriverManager::get_driver_by_name("GeoJson").unwrap();
    let mut dataset = json_driver.create_vector_only("/vsimem/polars_gdal/test_layer_from_df/layer.json").unwrap();

    let _layer = gdal_layer_from_df(&df, &mut dataset).unwrap();
    dataset.flush_cache();

    let mut json_bytes = vec![];
    gdal::vsi::call_on_mem_file_bytes("/vsimem/polars_gdal/test_layer_from_df/layer.json", |bytes| {
        json_bytes.extend_from_slice(bytes);
    }).unwrap();
    // println!("{}", String::from_utf8(json_bytes).unwrap());
}

#[test]
fn test_gdal_bytes_from_df() {
    use std::io::Cursor;
    use polars::prelude::IpcReader;

    let df_bytes = include_bytes!("../test_data/cities.arrow");
    let cursor = Cursor::new(df_bytes);

    let df = IpcReader::new(cursor).finish().unwrap();
    let json_driver = gdal::DriverManager::get_driver_by_name("GeoJson").unwrap();
    let geojson_bytes = gdal_bytes_from_df(&df, &json_driver).unwrap();
    println!("{}", String::from_utf8(geojson_bytes).unwrap());
}
