CREATE TABLE [dbo].[dv_satellite_column_keep] (
    [satellite_col_key] INT                IDENTITY (1, 1) NOT NULL,
    [satellite_key]     INT                NOT NULL,
    [column_key]        INT                NOT NULL,
    [release_key]       INT                NOT NULL,
    [version_number]    INT                NOT NULL,
    [updated_by]        VARCHAR (30)       NULL,
    [updated_datetime]  DATETIMEOFFSET (7) NULL
);

