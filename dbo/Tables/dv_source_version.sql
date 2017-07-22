CREATE TABLE [dbo].[dv_source_version] (
    [source_version_key]     INT                IDENTITY (1, 1) NOT NULL,
    [source_table_key]       INT                NOT NULL,
    [source_version]         INT                NULL,
    [source_type]            VARCHAR (50)       DEFAULT ('BespokeProc') NOT NULL,
    [source_procedure_name]  VARCHAR (128)      NULL,
    [source_filter]          VARCHAR (4000)     NULL,
    [pass_load_type_to_proc] BIT                DEFAULT ((0)) NOT NULL,
    [is_current]             BIT                CONSTRAINT [DF__dv_source__is_cu__414EAC47] DEFAULT ((1)) NOT NULL,
    [release_key]            INT                CONSTRAINT [DF__dv_source__relea__4242D080] DEFAULT ((0)) NOT NULL,
    [version_number]         INT                CONSTRAINT [DF__dv_source__versi__4336F4B9] DEFAULT ((1)) NULL,
    [updated_by]             VARCHAR (128)      CONSTRAINT [DF__dv_source__updat__442B18F2] DEFAULT (suser_name()) NULL,
    [update_date_time]       DATETIMEOFFSET (7) CONSTRAINT [DF__dv_source__updat__451F3D2B] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_source_version] PRIMARY KEY CLUSTERED ([source_version_key] ASC),
    CONSTRAINT [CK_dv_source_version__source_type] CHECK ([source_type]='BespokeProc' OR [source_type]='SourceTable' OR [source_type]='ExternalStage' OR [source_type]='LeftRightComparison' OR [source_type]='SSISPackage'),
    CONSTRAINT [FK__dv_source_version__dv_source_table] FOREIGN KEY ([source_table_key]) REFERENCES [dbo].[dv_source_table] ([source_table_key]),
    CONSTRAINT [FK_dv_source_version__dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key])
);




GO
CREATE UNIQUE NONCLUSTERED INDEX [dv_source_version_unique]
    ON [dbo].[dv_source_version]([source_table_key] ASC, [source_version] ASC);


GO
CREATE UNIQUE NONCLUSTERED INDEX [dv_source_version_unique_current]
    ON [dbo].[dv_source_version]([source_table_key] ASC) WHERE ([is_current]=(1));


GO
CREATE TRIGGER [dbo].[dv_source_version_audit] ON dbo.dv_source_version
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			 [update_date_time] = SYSDATETIMEOFFSET()
		   , [version_number] += 1
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_source_version] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[source_version_key] = [b].[source_version_key];
	END;