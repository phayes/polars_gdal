# geopolars_gdal

Read and write GDAL compatible file-formats into polars / geopolars.

Supports reading the following file-formats into GeoPolars:

1. GeoJSON
2. ShapeFiles
3. (Geo)Arrow and (Geo)Parquet (requires GDAL to be built with libarrow, run `gdalinfo --formats` to check)
5. FlatGeobuf
6. GeoRSS
7. GML
8. ... and [many more](https://gdal.org/drivers/vector/index.html)
