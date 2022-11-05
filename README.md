Read GDAL-compatible geospatial data into [Polars](https://www.pola.rs) and [GeoPolars](https://github.com/geopolars/geopolars).

Supports reading the following geospatial formats into a Polars Dataframe:

1. GeoJSON
2. ShapeFiles
3. CSV with lat / lon
4. FlatGeobuf
5. GeoRSS
6. GPX
7. PostGIS (via network)
8. SpatialLite
9. ... and [many more](https://gdal.org/drivers/vector/index.html)


### Example 1: Dataframe from a file
```rust # ignore
use geopolars_gdal::df_from_resource;
let df = df_from_resource("my_shapefile.shp", None).unwrap();
println!("{}", df);
```

### Example 2: DataFrame from raw bytes
```rust # ignore
use geopolars_gdal::df_from_bytes;

let geojson = r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"foo"},"geometry":{"type":"Point","coordinates":[1,2]}},{"type":"Feature","properties":{"name":"bar"},"geometry":{"type":"Point","coordinates":[3,4]}}]}"#.as_bytes().to_vec();

let df = df_from_bytes(geojson, None, None).unwrap();
println!("{}", df);
```

### Example 3: Dataframe from GDAL Layer with filtering query
```rust # ignore
use geopolars_gdal::{df_from_layer, gdal};
use gdal::vector::sql;

let dataset = gdal::Dataset::open("my_shapefile.shp")?;
let query = "SELECT kind, is_bridge, highway FROM my_shapefile WHERE highway = 'pedestrian'";
let mut result_set = dataset.execute_sql(query, None, sql::Dialect::DEFAULT).unwrap().unwrap();

let df = df_from_layer(&mut result_set, None).unwrap();
println!("{}", df);
```

### Example 4: Dataframe from Latitude / Longitude CSV with custom parsing options
```rust # ignore
let mut params = geopolars_gdal::Params::default();
let csv_parsing_options = ["EMPTY_STRING_AS_NULL=YES", "KEEP_GEOM_COLUMNS=NO", "X_POSSIBLE_NAMES=Lon*", "Y_POSSIBLE_NAMES=Lat*"];
params.open_options = Some(&csv_parsing_options);
 
let df = df_from_resource("lat_lon_countries.csv", Some(params)).unwrap();
println!("{}", df);
```

### Example 5: Dataframe from a PostGIS table
```rust # ignore
use geopolars_gdal::{df_from_resource, Params};

let mut params = Params::default();
params.layer_name = Some("some_table_name");
 
let df = df_from_resource("postgresql://user:pass@host/db_name", Some(params)).unwrap();
println!("{}", df);
```
