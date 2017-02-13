CREATE TABLE [dbo].[dv_hub] (
    [hub_key]          INT                IDENTITY (1, 1) NOT NULL,
    [hub_name]         VARCHAR (128)      NOT NULL,
    [hub_abbreviation] VARCHAR (4)        NULL,
    [hub_schema]       VARCHAR (128)      NOT NULL,
    [hub_database]     VARCHAR (128)      NOT NULL,
    [is_compressed]    BIT                DEFAULT ((0)) NOT NULL,
    [is_retired]       BIT                DEFAULT ((0)) NOT NULL,
    [release_key]      INT                CONSTRAINT [DF_dv_hub_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]   INT                CONSTRAINT [DF__dv_hub__version___534D60F1] DEFAULT ((1)) NOT NULL,
    [updated_by]       VARCHAR (30)       CONSTRAINT [DF__dv_hub__updated___5441852A] DEFAULT (suser_name()) NULL,
    [updated_datetime] DATETIMEOFFSET (7) CONSTRAINT [DF__dv_hub__updated___5535A963] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_hub__2671B43F8B7FC200] PRIMARY KEY CLUSTERED ([hub_key] ASC),
    CONSTRAINT [FK_dv_hub_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_hub_abr_unique] UNIQUE NONCLUSTERED ([hub_abbreviation] ASC),
    CONSTRAINT [dv_hub_unique] UNIQUE NONCLUSTERED ([hub_database] ASC, [hub_schema] ASC, [hub_name] ASC)
);




GO
CREATE TRIGGER [dbo].[dv_hub_audit] ON [dbo].[dv_hub]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[updated_datetime] = SYSDATETIMEOFFSET()
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_hub] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[hub_key] = [b].[hub_key];
	END;