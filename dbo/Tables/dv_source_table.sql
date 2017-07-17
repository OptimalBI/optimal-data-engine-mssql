CREATE TABLE [dbo].[dv_source_table] (
    [source_table_key]   INT                IDENTITY (1, 1) NOT NULL,
    [source_unique_name] VARCHAR (128)      NOT NULL,
    [load_type]          VARCHAR (50)       DEFAULT ('Full') NOT NULL,
    [system_key]         INT                NULL,
    [source_table_schma] VARCHAR (128)      NULL,
    [source_table_nme]   VARCHAR (128)      NULL,
    [stage_schema_key]   INT                NULL,
    [stage_table_name]   VARCHAR (128)      NULL,
    [is_columnstore]     BIT                DEFAULT ((0)) NOT NULL,
    [is_compressed]      BIT                DEFAULT ((0)) NOT NULL,
    [is_retired]         BIT                DEFAULT ((0)) NOT NULL,
    [release_key]        INT                DEFAULT ((0)) NOT NULL,
    [version_number]     INT                DEFAULT ((1)) NULL,
    [updated_by]         VARCHAR (30)       DEFAULT (suser_name()) NULL,
    [update_date_time]   DATETIMEOFFSET (7) DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_source_table] PRIMARY KEY CLUSTERED ([source_table_key] ASC),
    CONSTRAINT [CK_dv_source_table__load_type] CHECK ([load_type]='Full' OR [load_type]='Delta' OR [load_type]='ODEcdc' OR [load_type]='MSSQLcdc'),
    CONSTRAINT [FK__dv_source_table__dv_source_system] FOREIGN KEY ([system_key]) REFERENCES [dbo].[dv_source_system] ([source_system_key]),
    CONSTRAINT [FK_dv_source_table_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [FK_dv_source_table_dv_stage_schema] FOREIGN KEY ([stage_schema_key]) REFERENCES [dbo].[dv_stage_schema] ([stage_schema_key]),
    CONSTRAINT [dv_source_unique_name_unique] UNIQUE NONCLUSTERED ([source_unique_name] ASC),
    CONSTRAINT [dv_stage_table_unique] UNIQUE NONCLUSTERED ([stage_schema_key] ASC, [stage_table_name] ASC)
);






GO
CREATE TRIGGER [dbo].[dv_source_table_audit] ON [dbo].[dv_source_table]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			 [update_date_time] = SYSDATETIMEOFFSET()
		   , [version_number] += 1
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_source_table] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[source_table_key] = [b].[source_table_key];
	END;
GO
CREATE NONCLUSTERED INDEX [IX_dv_source_table_unique_source]
    ON [dbo].[dv_source_table]([system_key] ASC, [source_table_schma] ASC, [source_table_nme] ASC);



