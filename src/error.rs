use thiserror::Error;
use gdal::errors::GdalError;
use polars::error::PolarsError as PolarsError;

#[derive(Error, Debug)] 
pub enum Error {
    #[error("GDAL Error: {0}")]
    Gdal(#[from] GdalError),

    #[error("Polars Error: {0}")]
    Polars(#[from] PolarsError),

    #[error("Empty data")]
    EmptyData,

    #[error("GDAL READONLY bitflag must be set for this operation.")]
    ReadonlyMustSet,

    #[error("GDAL update bitfla is not supported for this operation.")]
    UpdateNotSupported,

    #[error("Feature limit of {0} features reached")]
    FeatureLimitReached(usize),

    #[error("Field named `{0}` not allowed as it would conflict with the geometry column")]
    GeometryColumnCollision(String),

    #[error("Field named `{0}` not allowed as it would conflict with the feature id column")]
    FidColumnCollision(String),
}
