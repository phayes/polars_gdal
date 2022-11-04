use gdal::vector::FieldValue as GdalValue;
use polars::prelude::*;

#[derive(Debug)]
pub(crate) enum GdalData {
    Value(Option<gdal::vector::FieldValue>),
    Geometry(Vec<u8>),
    Fid(u64),
}

#[derive(Debug)]
pub(crate) enum UnprocessedDataType {
    Integer,
    IntegerList,
    Integer64,
    Integer64List,
    String,
    StringList,
    Real,
    RealList,
    Date,
    DateTime,
    Null,
    Geometry,
    Fid,
}

pub(crate) fn gdal_type_to_unprocessed_type(gdal_type: &Option<gdal::vector::FieldValue>) -> UnprocessedDataType {
    match gdal_type {
        Some(gdal::vector::FieldValue::IntegerValue(_)) => UnprocessedDataType::Integer,
        Some(gdal::vector::FieldValue::IntegerListValue(_)) => UnprocessedDataType::IntegerList,
        Some(gdal::vector::FieldValue::Integer64Value(_)) => UnprocessedDataType::Integer64,
        Some(gdal::vector::FieldValue::Integer64ListValue(_)) => UnprocessedDataType::Integer64List,
        Some(gdal::vector::FieldValue::StringValue(_)) => UnprocessedDataType::String,
        Some(gdal::vector::FieldValue::StringListValue(_)) => UnprocessedDataType::StringList,
        Some(gdal::vector::FieldValue::RealValue(_)) => UnprocessedDataType::Real,
        Some(gdal::vector::FieldValue::RealListValue(_)) => UnprocessedDataType::RealList,
        Some(gdal::vector::FieldValue::DateValue(_)) => UnprocessedDataType::Date,
        Some(gdal::vector::FieldValue::DateTimeValue(_)) => UnprocessedDataType::DateTime,
        None => UnprocessedDataType::Null,
    }
}

pub(crate) struct UnprocessedSeries {
    pub(crate) name: String,
    pub(crate) datatype: UnprocessedDataType,
    pub(crate) nullable: bool,
    pub(crate) data: Vec<GdalData>,
}

impl UnprocessedSeries {
    pub(crate) fn process(self) -> Series {
        let mut series = if self.nullable {
            match self.datatype {
                UnprocessedDataType::String => {
                    let ca: Utf8Chunked = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::StringValue(val))) => Some(val),
                            GdalData::Value(None) => None,
                            _ => unreachable!("geopadas_gdal: Unexpected non-string value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    ca.into_series()
                },
                UnprocessedDataType::Integer => {
                    let vec: Vec<Option<i32>> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::IntegerValue(val))) => Some(val),
                            GdalData::Value(None) => None,
                            _ => unreachable!("geopadas_gdal: Unexpected non-i32 value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Integer64 => {
                    let vec: Vec<Option<i64>> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::Integer64Value(val))) => Some(val),
                            GdalData::Value(None) => None,
                            _ => unreachable!("geopadas_gdal: Unexpected non-i64 value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Real => {
                    let vec: Vec<Option<f64>> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::RealValue(val))) => Some(val),
                            GdalData::Value(None) => None,
                            _ => unreachable!("geopadas_gdal: Unexpected non-f64 value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Null => panic!("geopandas_gdal: Unexpected null value in {}", &self.name),
                _ => unimplemented!("geopandas_gdal: Still need to implement Lists and Dates"),
            }
        } else {
            match self.datatype {
                UnprocessedDataType::String => {
                    let vec: Vec<String> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::StringValue(val))) => val,
                            _ => unreachable!("geopadas_gdal: Unexpected non-string value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Integer => {
                    let vec: Vec<i32> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::IntegerValue(val))) => val,
                            _ => unreachable!("geopadas_gdal: Unexpected non-i32 value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Integer64 => {
                    let vec: Vec<i64> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::Integer64Value(val))) => val,
                            _ => unreachable!("geopadas_gdal: Unexpected non-i64 value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Real => {
                    let vec: Vec<f64> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::RealValue(val))) => val,
                            _ => unreachable!("geopadas_gdal: Unexpected non-f64 value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Geometry => {
                    let ca: BinaryChunked = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Geometry(val) => val,
                            _ => unreachable!("geopadas_gdal: Unexpected non-geometry value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    ca.into_series()
                },
                UnprocessedDataType::Fid => {
                    let vec: Vec<u64> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Fid(val) => val,
                            _ => unreachable!("geopadas_gdal: Unexpected non-u64 fid value `{:?}` in {}", &v, &self.name),
                        })
                        .collect();
                    Series::from_iter(vec)
                },
                UnprocessedDataType::Null => panic!("geopandas_gdal: Unexpected null value in {}", &self.name),
                _ => unimplemented!("geopandas_gdal: Still need to implement Lists and Dates"),
            }
        };

        series.rename(&self.name);

        series
    }
}