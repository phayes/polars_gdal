use thiserror::Error;
use gdal::errors::GdalError;
use polars::error::PolarsError as PolarsError;

#[derive(Error, Debug)] 
pub enum Error {

    /// GDAL Error
    #[error("GDAL Error: {0}")]
    Gdal(#[from] GdalError),

    /// Polars Error
    #[error("Polars Error: {0}")]
    Polars(#[from] PolarsError),

    /// Empty GDAL dataset
    #[error("Empty GDAL data")]
    EmptyData,

    /// The readonly bitflag must be set
    #[error("GDAL READONLY bitflag must be set for this operation.")]
    ReadonlyMustSet,

    /// The update bitflag must NOT be set
    #[error("GDAL update bitfla is not supported for this operation.")]
    UpdateNotSupported,

    //// Hard feature limit reached
    #[error("Feature limit of {0} features reached")]
    FeatureLimitReached(usize),

    /// Geomery column name collision
    #[error("Field named `{0}` not allowed as it would conflict with the geometry column")]
    GeometryColumnCollision(String),

    /// Feature ID column name collision
    #[error("Field named `{0}` not allowed as it would conflict with the feature id column")]
    FidColumnCollision(String),

    /// Geometry column was the wrong type.
    #[error("The dataframe geometry column `{0}` was not the right type. Expected type `{1}`, got type `{2}`.")]
    GeometryColumnWrongType(String, polars::datatypes::DataType, polars::datatypes::DataType),

    /// Unable to automatically determine geometry type.
    #[error("Unable to automatically determine the the geometry type from the first row. Got Error \"{0}\". Hint: Use `polars_gdal::WriteParams::geometry_type` to specify manually.")]
    UnableToDetermineGeometryType(String),

    /// Empty Dataframe
    #[error("Empty dataframe with no rows")]
    EmptyDataframe,

    /// Cannot find geometry column in dataframe
    #[error("Cannot find geometry column `{0}` in dataframe")]
    CannotFindGeometryColumn(String),
}
