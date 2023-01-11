use gdal::vector::FieldValue as GdalValue;
use polars::export::chrono;
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
    GeometryWKB,
    Fid,
}

pub(crate) fn gdal_type_to_unprocessed_type(
    gdal_type: &Option<gdal::vector::FieldValue>,
) -> UnprocessedDataType {
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
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-string value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    ca.into_series()
                }
                UnprocessedDataType::Integer => {
                    let vec: Vec<Option<i32>> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::IntegerValue(val))) => Some(val),
                            GdalData::Value(None) => None,
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-i32 value `{:?}` in {}",
                                &v, &self.name
                            ),
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
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-i64 value `{:?}` in {}",
                                &v, &self.name
                            ),
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
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-f64 value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Date => {
                    let vec: Vec<Option<chrono::NaiveDate>> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::DateValue(val))) => {
                                Some(val.naive_utc())
                            }
                            GdalData::Value(None) => None,
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-date value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    let ca = DateChunked::from_naive_date_options(&self.name, vec);
                    ca.into_series()
                }
                UnprocessedDataType::DateTime => {
                    let vec: Vec<Option<chrono::NaiveDateTime>> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::DateTimeValue(val))) => {
                                Some(val.naive_utc())
                            }
                            GdalData::Value(None) => None,
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-date value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    let ca = DatetimeChunked::from_naive_datetime_options(&self.name, vec, TimeUnit::Nanoseconds);
                    ca.into_series()
                }
                UnprocessedDataType::GeometryWKB => {
                    let ca: BinaryChunked = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Geometry(val) => Some(val),
                            GdalData::Value(None) => None,
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-geometry value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    ca.into_series()
                }
                UnprocessedDataType::Null => {
                    panic!("geopolars_gdal: Unexpected null value in {}", &self.name)
                }
                _ => unimplemented!("geopolars_gdal: Error processing {} - Still need to implement Lists", self.name),
            }
        } else {
            match self.datatype {
                UnprocessedDataType::String => {
                    let vec: Vec<String> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::StringValue(val))) => val,
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-string value `{:?}` in {}",
                                &v, &self.name
                            ),
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
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-i32 value `{:?}` in {}",
                                &v, &self.name
                            ),
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
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-i64 value `{:?}` in {}",
                                &v, &self.name
                            ),
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
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-f64 value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Date => {
                    let vec: Vec<chrono::NaiveDate> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::DateValue(val))) => val.naive_utc(),
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-date value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    let ca = DateChunked::from_naive_date(&self.name, vec);
                    ca.into_series()
                }
                UnprocessedDataType::DateTime => {
                    let vec: Vec<chrono::NaiveDateTime> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Value(Some(GdalValue::DateTimeValue(val))) => val.naive_utc(),
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-date value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    let ca = DatetimeChunked::from_naive_datetime(
                        &self.name,
                        vec,
                        TimeUnit::Nanoseconds,
                    );
                    ca.into_series()
                }
                UnprocessedDataType::GeometryWKB => {
                    let ca: BinaryChunked = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Geometry(val) => val,
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-geometry value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    ca.into_series()
                }
                UnprocessedDataType::Fid => {
                    let vec: Vec<u64> = self
                        .data
                        .into_iter()
                        .map(|v| match v {
                            GdalData::Fid(val) => val,
                            _ => unreachable!(
                                "geopadas_gdal: Unexpected non-u64 fid value `{:?}` in {}",
                                &v, &self.name
                            ),
                        })
                        .collect();
                    Series::from_iter(vec)
                }
                UnprocessedDataType::Null => {
                    panic!("geopolars_gdal: Unexpected null value in {}", &self.name)
                }
                _ => unimplemented!(
                    "geopolars_gdal: Error processing {} - Still need to implement Lists",
                    self.name
                ),
            }
        };

        series.rename(&self.name);

        series
    }
}
