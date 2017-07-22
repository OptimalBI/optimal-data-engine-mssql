CREATE TABLE [dbo].[dv_column_match] (
    [col_match_key]             INT                IDENTITY (1, 1) NOT NULL,
    [match_key]                 INT                NOT NULL,
    [left_hub_key_column_key]   INT                NULL,
    [left_link_key_column_key]  INT                NULL,
    [left_satellite_col_key]    INT                NULL,
    [left_column_key]           INT                NULL,
    [right_hub_key_column_key]  INT                NULL,
    [right_link_key_column_key] INT                NULL,
    [right_satellite_col_key]   INT                NULL,
    [right_column_key]          INT                NULL,
    [release_key]               INT                DEFAULT ((0)) NOT NULL,
    [version_number]            INT                DEFAULT ((1)) NOT NULL,
    [updated_by]                VARCHAR (128)      DEFAULT (suser_name()) NULL,
    [updated_datetime]          DATETIMEOFFSET (7) DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_column_match] PRIMARY KEY CLUSTERED ([col_match_key] ASC),
    CONSTRAINT [FK__dv_column_match__dv_column_left] FOREIGN KEY ([left_column_key]) REFERENCES [dbo].[dv_column] ([column_key]),
    CONSTRAINT [FK__dv_column_match__dv_column_right] FOREIGN KEY ([right_column_key]) REFERENCES [dbo].[dv_column] ([column_key]),
    CONSTRAINT [FK__dv_column_match__dv_hub_key_column_left] FOREIGN KEY ([left_hub_key_column_key]) REFERENCES [dbo].[dv_hub_key_column] ([hub_key_column_key]),
    CONSTRAINT [FK__dv_column_match__dv_hub_key_column_right] FOREIGN KEY ([right_hub_key_column_key]) REFERENCES [dbo].[dv_hub_key_column] ([hub_key_column_key]),
    CONSTRAINT [FK__dv_column_match__dv_link_key_column_left] FOREIGN KEY ([left_link_key_column_key]) REFERENCES [dbo].[dv_link_key_column] ([link_key_column_key]),
    CONSTRAINT [FK__dv_column_match__dv_link_key_column_right] FOREIGN KEY ([right_link_key_column_key]) REFERENCES [dbo].[dv_link_key_column] ([link_key_column_key]),
    CONSTRAINT [FK__dv_column_match__dv_object_match] FOREIGN KEY ([match_key]) REFERENCES [dbo].[dv_object_match] ([match_key]),
    CONSTRAINT [FK__dv_column_match__dv_satellite_column_left] FOREIGN KEY ([left_satellite_col_key]) REFERENCES [dbo].[dv_satellite_column] ([satellite_col_key]),
    CONSTRAINT [FK__dv_column_match__dv_satellite_column_right] FOREIGN KEY ([right_satellite_col_key]) REFERENCES [dbo].[dv_satellite_column] ([satellite_col_key]),
    CONSTRAINT [FK_dv_column_match_dv__release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_dv_column_match_unique] UNIQUE NONCLUSTERED ([match_key] ASC, [left_hub_key_column_key] ASC, [left_link_key_column_key] ASC, [left_satellite_col_key] ASC, [left_column_key] ASC, [right_hub_key_column_key] ASC, [right_link_key_column_key] ASC, [right_satellite_col_key] ASC, [right_column_key] ASC)
);


GO


CREATE TRIGGER [dbo].[dv_column_match_audit] ON [dbo].[dv_column_match]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			 [updated_datetime] = SYSDATETIMEOFFSET()
		   , [version_number] += 1
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_column_match] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[col_match_key] = [b].[col_match_key];
	END;