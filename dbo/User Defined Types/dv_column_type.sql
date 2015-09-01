CREATE TYPE [dbo].[dv_column_type] AS TABLE (
    [column_name]                VARCHAR (128) NOT NULL,
    [column_type]                VARCHAR (50)  NOT NULL,
    [column_length]              INT           NULL,
    [column_precision]           INT           NULL,
    [column_scale]               INT           NULL,
    [collation_name]             [sysname]     NULL,
    [bk_ordinal_position]        INT           DEFAULT ((0)) NOT NULL,
    [source_ordinal_position]    INT           NOT NULL,
    [satellite_ordinal_position] INT           NOT NULL,
    [abbreviation]               VARCHAR (50)  NULL,
    [object_type]                VARCHAR (50)  NULL);

