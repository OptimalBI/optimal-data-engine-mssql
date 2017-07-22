CREATE TABLE [dbo].[dv_link] (
    [link_key]          INT                IDENTITY (1, 1) NOT NULL,
    [link_name]         VARCHAR (128)      NOT NULL,
    [link_abbreviation] VARCHAR (4)        NULL,
    [link_schema]       VARCHAR (128)      NOT NULL,
    [link_database]     VARCHAR (128)      NOT NULL,
    [is_retired]        BIT                DEFAULT ((0)) NOT NULL,
    [is_compressed]     BIT                DEFAULT ((0)) NOT NULL,
    [release_key]       INT                CONSTRAINT [DF_dv_link_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]    INT                CONSTRAINT [DF__dv_link__version__6C190EBB] DEFAULT ((1)) NOT NULL,
    [updated_by]        VARCHAR (128)      CONSTRAINT [DF__dv_link__updated__6D0D32F4] DEFAULT (suser_name()) NULL,
    [updated_datetime]  DATETIMEOFFSET (7) CONSTRAINT [DF__dv_link__updated__6E01572D] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_link__8F1D0002234E32BF] PRIMARY KEY CLUSTERED ([link_key] ASC),
    CONSTRAINT [FK_dv_link_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_link_abr_unique] UNIQUE NONCLUSTERED ([link_abbreviation] ASC),
    CONSTRAINT [dv_link_unique] UNIQUE NONCLUSTERED ([link_database] ASC, [link_schema] ASC, [link_name] ASC)
);




GO


CREATE TRIGGER [dbo].[dv_link_audit] ON [dbo].[dv_link]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[updated_datetime] = SYSDATETIMEOFFSET()
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_link] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[link_key] = [b].[link_key];
	END;