CREATE TABLE [dbo].[dv_object_match] (
    [match_key]          INT                IDENTITY (1, 1) NOT NULL,
    [source_version_key] INT                NOT NULL,
    [temporal_pit_left]  DATETIMEOFFSET (7) NULL,
    [temporal_pit_right] DATETIMEOFFSET (7) NULL,
    [is_retired]         BIT                DEFAULT ((0)) NOT NULL,
    [release_key]        INT                DEFAULT ((0)) NOT NULL,
    [version_number]     INT                DEFAULT ((1)) NOT NULL,
    [updated_by]         VARCHAR (128)      DEFAULT (suser_name()) NULL,
    [updated_datetime]   DATETIMEOFFSET (7) DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_object_match] PRIMARY KEY CLUSTERED ([match_key] ASC),
    CONSTRAINT [FK_dv_object_match__dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [FK_dv_object_match__dv_source_version] FOREIGN KEY ([source_version_key]) REFERENCES [dbo].[dv_source_version] ([source_version_key])
);


GO


CREATE TRIGGER [dbo].[dv_object_match_audit] ON [dbo].[dv_object_match]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			 [updated_datetime] = SYSDATETIMEOFFSET()
		   , [version_number] += 1
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_object_match] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[match_key] = [b].[match_key];
	END;