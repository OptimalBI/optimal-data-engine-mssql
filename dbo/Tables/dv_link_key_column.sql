CREATE TABLE [dbo].[dv_link_key_column] (
    [link_key_column_key]  INT                IDENTITY (1, 1) NOT NULL,
    [link_key]             INT                NOT NULL,
    [link_key_column_name] VARCHAR (128)      NOT NULL,
    [release_key]          INT                CONSTRAINT [DF_dv_link_key_column_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]       INT                CONSTRAINT [DF__dv_link_key_column__version] DEFAULT ((1)) NOT NULL,
    [updated_by]           VARCHAR (30)       CONSTRAINT [DF__dv_link_key_column__updated_by] DEFAULT (suser_name()) NULL,
    [updated_datetime]     DATETIMEOFFSET (7) CONSTRAINT [DF__dv_link_key_column__updated_datetime] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_link_key_column] PRIMARY KEY CLUSTERED ([link_key_column_key] ASC),
    CONSTRAINT [FK__dv_link_key_column__dv_link] FOREIGN KEY ([link_key]) REFERENCES [dbo].[dv_link] ([link_key]),
    CONSTRAINT [FK_dv_link_key_column_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_link_column_key_unique] UNIQUE NONCLUSTERED ([link_key] ASC, [link_key_column_name] ASC)
);


GO

CREATE TRIGGER [dbo].[dv_link_key_column_audit] ON [dbo].[dv_link_key_column]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[updated_datetime] = SYSDATETIMEOFFSET()
		   ,[updated_by] = SUSER_NAME() FROM [dbo].[dv_link_key_column] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[link_key_column_key] = [b].[link_key_column_key];
	END;