CREATE TYPE [dbo].[dv_column_list] AS TABLE (
    [column_name]      NVARCHAR (128) NULL,
    [ordinal_position] INT            IDENTITY (1, 1) NOT NULL,
    PRIMARY KEY CLUSTERED ([ordinal_position] ASC));

