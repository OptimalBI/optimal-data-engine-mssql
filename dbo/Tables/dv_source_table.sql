CREATE TABLE [dbo].[dv_source_table] (
    [source_table_key]        INT                IDENTITY (1, 1) NOT NULL,
    [system_key]              INT                NOT NULL,
    [source_table_schema]     VARCHAR (128)      NOT NULL,
    [source_table_name]       VARCHAR (128)      NOT NULL,
    [source_table_load_type]  VARCHAR (50)       CONSTRAINT [DF__dv_source__sourc__07C12930] DEFAULT ('Full') NOT NULL,
    [source_procedure_schema] VARCHAR (128)      NULL,
    [source_procedure_name]   VARCHAR (128)      NULL,
    [is_retired]              BIT                DEFAULT ((0)) NOT NULL,
    [release_key]             INT                CONSTRAINT [DF_dv_source_table_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]          INT                CONSTRAINT [DF__dv_source__versi__08B54D69] DEFAULT ((1)) NULL,
    [updated_by]              VARCHAR (30)       CONSTRAINT [DF__dv_source__updat__09A971A2] DEFAULT (suser_name()) NULL,
    [update_date_time]        DATETIMEOFFSET (7) CONSTRAINT [DF__dv_source__updat__0A9D95DB] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_sourc__D64CD3B7E8292317] PRIMARY KEY CLUSTERED ([source_table_key] ASC),
    CONSTRAINT [FK__dv_source_table__dv_source_system] FOREIGN KEY ([system_key]) REFERENCES [dbo].[dv_source_system] ([source_system_key]),
    CONSTRAINT [FK_dv_source_table_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_source_system_unique] UNIQUE NONCLUSTERED ([system_key] ASC, [source_table_schema] ASC, [source_table_name] ASC)
);


GO
CREATE TRIGGER [dbo].[dv_source_table_audit] ON [dbo].[dv_source_table]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[update_date_time] = SYSDATETIMEOFFSET()
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_source_table] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[source_table_key] = [b].[source_table_key];
	END;