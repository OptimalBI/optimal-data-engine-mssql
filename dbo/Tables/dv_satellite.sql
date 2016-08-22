CREATE TABLE [dbo].[dv_satellite] (
    [satellite_key]               INT                IDENTITY (1, 1) NOT NULL,
    [hub_key]                     INT                CONSTRAINT [DF__dv_satell__hub_k__72C60C4A] DEFAULT ((0)) NOT NULL,
    [link_key]                    INT                CONSTRAINT [DF__dv_satell__link___73BA3083] DEFAULT ((0)) NOT NULL,
    [link_hub_satellite_flag]     CHAR (1)           CONSTRAINT [DF__dv_satell__link___74AE54BC] DEFAULT ('H') NOT NULL,
    [satellite_name]              VARCHAR (128)      NOT NULL,
    [satellite_abbreviation]      VARCHAR (4)        NULL,
    [satellite_schema]            VARCHAR (128)      NOT NULL,
    [satellite_database]          VARCHAR (128)      NOT NULL,
    [duplicate_removal_threshold] INT                CONSTRAINT [DF_dv_satellite_duplicate_removal_threshold] DEFAULT ((0)) NOT NULL,
    [is_columnstore]              BIT                CONSTRAINT [DF__dv_satell__is_co__75A278F5] DEFAULT ((0)) NOT NULL,
    [is_retired]                  BIT                DEFAULT ((0)) NOT NULL,
    [release_key]                 INT                CONSTRAINT [DF_dv_satellite_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]              INT                CONSTRAINT [DF__dv_satell__versi__76969D2E] DEFAULT ((1)) NOT NULL,
    [updated_by]                  VARCHAR (30)       CONSTRAINT [DF__dv_satell__updat__778AC167] DEFAULT (suser_name()) NULL,
    [updated_datetime]            DATETIMEOFFSET (7) CONSTRAINT [DF__dv_satell__updat__787EE5A0] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_satel__591F7B98AC67FA20] PRIMARY KEY CLUSTERED ([satellite_key] ASC),
    CONSTRAINT [FK__dv_satellite__dv_hub] FOREIGN KEY ([hub_key]) REFERENCES [dbo].[dv_hub] ([hub_key]),
    CONSTRAINT [FK__dv_satellite__dv_link] FOREIGN KEY ([link_key]) REFERENCES [dbo].[dv_link] ([link_key]),
    CONSTRAINT [FK_dv_satellite_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_sat_abr_unique] UNIQUE NONCLUSTERED ([satellite_abbreviation] ASC),
    CONSTRAINT [dv_satellite_unique] UNIQUE NONCLUSTERED ([satellite_name] ASC),
	CONSTRAINT [CK_dv_satellite__link_hub_flag] CHECK ([link_hub_satellite_flag]='H' OR [link_hub_satellite_flag]='L')
);


GO

CREATE TRIGGER [dbo].[dv_satellite_audit] ON [dbo].[dv_satellite]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[updated_datetime] = SYSDATETIMEOFFSET()
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_satellite] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[satellite_key] = [b].[satellite_key];
	END;