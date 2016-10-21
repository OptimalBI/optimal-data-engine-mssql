CREATE TABLE [dbo].[dv_satellite_column_keep] (
    [satellite_col_key]          INT                IDENTITY (1, 1) NOT NULL,
    [satellite_key]              INT                NOT NULL,
    [column_key]                 INT                NOT NULL,
    [column_name]                VARCHAR (128)      NOT NULL,
    [column_type]                VARCHAR (30)       NOT NULL,
    [column_length]              INT                NULL,
    [column_precision]           INT                NULL,
    [column_scale]               INT                NULL,
    [collation_name]             [sysname]          NULL,
    [satellite_ordinal_position] INT                NOT NULL,
    [ref_function_key]           INT                NOT NULL,
    [func_arguments]             NVARCHAR (512)     NULL,
    [func_ordinal_position]      INT                NOT NULL,
    [release_key]                INT                NOT NULL,
    [version_number]             INT                NOT NULL,
    [updated_by]                 VARCHAR (30)       NULL,
    [updated_datetime]           DATETIMEOFFSET (7) NULL
);

