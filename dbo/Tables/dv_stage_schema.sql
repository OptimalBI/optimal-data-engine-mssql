CREATE TABLE [dbo].[dv_stage_schema] (
    [stage_schema_key]   INT                IDENTITY (1, 1) NOT NULL,
    [stage_database_key] INT                NOT NULL,
    [stage_schema_name]  VARCHAR (50)       NOT NULL,
    [is_retired]         BIT                DEFAULT ((0)) NOT NULL,
    [release_key]        INT                DEFAULT ((0)) NOT NULL,
    [version_number]     INT                DEFAULT ((1)) NULL,
    [updated_by]         VARCHAR (128)      DEFAULT (suser_name()) NULL,
    [update_date_time]   DATETIMEOFFSET (7) DEFAULT (sysdatetimeoffset()) NULL,
    PRIMARY KEY CLUSTERED ([stage_schema_key] ASC),
    CONSTRAINT [FK_dv_stage_schema__dv_stage_database] FOREIGN KEY ([stage_database_key]) REFERENCES [dbo].[dv_stage_database] ([stage_database_key]),
    CONSTRAINT [stage_schema_unique] UNIQUE NONCLUSTERED ([stage_database_key] ASC, [stage_schema_name] ASC)
);


GO

CREATE TRIGGER [dbo].[dv_stage_schema_audit] ON [dbo].[dv_stage_schema]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			 [update_date_time] = SYSDATETIMEOFFSET()
		   , [version_number] += 1
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_stage_schema] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[stage_schema_key] = [b].[stage_schema_key];
	END;