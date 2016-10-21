CREATE TABLE [dbo].[dv_satellite_column] (
    [satellite_col_key]          INT                IDENTITY (1, 1) NOT NULL,
    [satellite_key]              INT                NOT NULL,
    [column_name]                VARCHAR (128)      NOT NULL,
    [column_type]                VARCHAR (30)       NOT NULL,
    [column_length]              INT                NULL,
    [column_precision]           INT                NULL,
    [column_scale]               INT                NULL,
    [collation_name]             [sysname]          NULL,
    [satellite_ordinal_position] INT                NOT NULL,
    [ref_function_key]           INT                CONSTRAINT [DF_dv_satellite_column_ref_function_key] DEFAULT ((0)) NOT NULL,
    [func_arguments]             NVARCHAR (512)     NULL,
    [func_ordinal_position]      INT                CONSTRAINT [DF_dv_satellite_column_func_ordinal_position] DEFAULT ((0)) NOT NULL,
    [release_key]                INT                CONSTRAINT [DF_dv_satellite_column_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]             INT                CONSTRAINT [DF_dv_satellite_column_version_number] DEFAULT ((1)) NOT NULL,
    [updated_by]                 VARCHAR (30)       CONSTRAINT [DF_dv_satellite_column_updated_by] DEFAULT (suser_name()) NULL,
    [updated_datetime]           DATETIMEOFFSET (7) CONSTRAINT [DF_dv_satellite_column_updated_datetime] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_satellite_column] PRIMARY KEY CLUSTERED ([satellite_col_key] ASC),
    CONSTRAINT [FK__dv_satellite_column__dv_ref_function] FOREIGN KEY ([ref_function_key]) REFERENCES [dbo].[dv_ref_function] ([ref_function_key]),
    CONSTRAINT [FK__dv_satellite_column__dv_satellite] FOREIGN KEY ([satellite_key]) REFERENCES [dbo].[dv_satellite] ([satellite_key]),
    CONSTRAINT [FK_dv_satellite_column_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_satellite_column_unique] UNIQUE NONCLUSTERED ([satellite_key] ASC, [column_name] ASC)
);










GO


CREATE TRIGGER [dbo].[dv_satellite_column_audit] ON [dbo].[dv_satellite_column]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[updated_datetime] = SYSDATETIMEOFFSET()
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_satellite_column] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[satellite_col_key] = [b].[satellite_col_key];
	END;