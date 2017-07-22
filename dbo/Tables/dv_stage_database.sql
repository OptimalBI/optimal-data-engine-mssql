CREATE TABLE [dbo].[dv_stage_database] (
    [stage_database_key]    INT                IDENTITY (1, 1) NOT NULL,
    [stage_database_name]   VARCHAR (50)       NOT NULL,
    [stage_connection_name] VARCHAR (50)       NULL,
    [is_retired]            BIT                DEFAULT ((0)) NOT NULL,
    [release_key]           INT                DEFAULT ((0)) NOT NULL,
    [version_number]        INT                DEFAULT ((1)) NULL,
    [updated_by]            VARCHAR (128)      DEFAULT (suser_name()) NULL,
    [update_date_time]      DATETIMEOFFSET (7) DEFAULT (sysdatetimeoffset()) NULL,
    PRIMARY KEY CLUSTERED ([stage_database_key] ASC),
    CONSTRAINT [stage_database_unique] UNIQUE NONCLUSTERED ([stage_database_name] ASC)
);




GO

CREATE TRIGGER [dbo].[dv_stage_database_audit] ON [dbo].[dv_stage_database]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			 [update_date_time] = SYSDATETIMEOFFSET()
		   , [version_number] += 1
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_stage_database] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[stage_database_key] = [b].[stage_database_key];
	END;