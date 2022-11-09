Read and write GDAL-compatible geospatial data into [Polars](https://www.pola.rs) and [GeoPolars](https://github.com/geopolars/geopolars).

Supports reading and writing the following geospatial formats into / from a Polars Dataframe:

1. GeoJSON
2. ShapeFiles
3. CSV with lat / lon
4. FlatGeobuf
5. KML
6. GPX
7. PostGIS (via network)
8. SpatialLite
9. ... and [many more](https://gdal.org/drivers/vector/index.html)


### Example 1: Dataframe from a file
```rust # ignore
use polars_gdal::df_from_resource;
let df = df_from_resource("my_shapefile.shp", None).unwrap();
println!("{}", df);
```

### Example 2: DataFrame from raw bytes
```rust # ignore
use polars_gdal::df_from_bytes;

let geojson = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"foo"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"bar"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#.as_bytes();

let df = df_from_bytes(geojson, None, None).unwrap();
println!("{}", df);
```

### Example 3: DataFrame from GDAL Layer with filtering query
```rust # ignore
use polars_gdal::{df_from_layer, gdal};
use gdal::vector::sql;

let dataset = gdal::Dataset::open("my_shapefile.shp")?;
let query = "SELECT kind, is_bridge, highway FROM my_shapefile WHERE highway = 'pedestrian'";
let mut result_set = dataset.execute_sql(query, None, sql::Dialect::DEFAULT).unwrap().unwrap();

let df = df_from_layer(&mut result_set, None).unwrap();
println!("{}", df);
```

### Example 4: DataFrame from Latitude / Longitude CSV with custom parsing options
```rust # ignore
let mut params = polars_gdal::Params::default();
let csv_parsing_options = ["EMPTY_STRING_AS_NULL=YES", "KEEP_GEOM_COLUMNS=NO", "X_POSSIBLE_NAMES=Lon*", "Y_POSSIBLE_NAMES=Lat*"];
params.open_options = Some(&csv_parsing_options);
 
let df = df_from_resource("lat_lon_countries.csv", Some(params)).unwrap();
println!("{}", df);
```

### Example 5: DataFrame from a PostGIS table
```rust # ignore
use polars_gdal::{df_from_resource, Params};

let mut params = Params::default();
params.layer_name = Some("some_table_name");
 
let df = df_from_resource("postgresql://user:pass@host/db_name", Some(params)).unwrap();
println!("{}", df);
```

### Example 6: GeoJSON bytes from a Dataframe
```rust # ignore
use polars_gdal::{gdal, gdal_bytes_from_df, WriteParams};

let df: DataFrame = ...;
let json_driver = gdal::DriverManager::get_driver_by_name("GeoJson")?;
let geojson_bytes = gdal_bytes_from_df(&df, &json_driver)?;
```

### Example 7: Write a shapefile to disk from a DataFrame
```rust # ignore
use polars_gdal::{gdal, gdal_dataset_from_df, WriteParams};

let df: DataFrame = ...;
let shapefile_driver = gdal::DriverManager::get_driver_by_name("ESRI Shapefile")?;
let _dataset = gdal_dataset_from_df(&df, &shapefile_driver, "/some/path/on/disk/my_shapefile.shp")?;
```
