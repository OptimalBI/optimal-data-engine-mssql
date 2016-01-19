CREATE TABLE [dbo].[dv_satellite] (
    [satellite_key]               INT                IDENTITY (1, 1) NOT NULL,
    [hub_key]                     INT                CONSTRAINT [DF__dv_satell__hub_k__72C60C4A] DEFAULT ((0)) NOT NULL,
    [link_key]                    INT                CONSTRAINT [DF__dv_satell__link___73BA3083] DEFAULT ((0)) NOT NULL,
    [link_hub_satellite_flag]     CHAR (1)           CONSTRAINT [DF__dv_satell__link___74AE54BC] DEFAULT ('H') NOT NULL,
    [satellite_name]              VARCHAR (128)      NOT NULL,
    [satellite_abbreviation]      VARCHAR (4)        NOT NULL,
    [satellite_schema]            VARCHAR (128)      NULL,
    [satellite_database]          VARCHAR (128)      NOT NULL,
    [satellite_filegroup]         VARCHAR (128)      NULL,
    [duplicate_removal_threshold] INT                CONSTRAINT [DF_dv_satellite_duplicate_removal_threshold] DEFAULT ((0)) NOT NULL,
    [hashmatching_type]           VARCHAR (10)       CONSTRAINT [DF__dv_sat__hash] DEFAULT ('None') NULL,
    [is_columnstore]              BIT                CONSTRAINT [DF__dv_satell__is_co__75A278F5] DEFAULT ((0)) NOT NULL,
    [release_key]                 INT                CONSTRAINT [DF_dv_satellite_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]              INT                CONSTRAINT [DF__dv_satell__versi__76969D2E] DEFAULT ((1)) NOT NULL,
    [updated_by]                  VARCHAR (30)       CONSTRAINT [DF__dv_satell__updat__778AC167] DEFAULT (suser_name()) NULL,
    [updated_datetime]            DATETIMEOFFSET (7) CONSTRAINT [DF__dv_satell__updat__787EE5A0] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_satel__591F7B98AC67FA20] PRIMARY KEY CLUSTERED ([satellite_key] ASC),
    CONSTRAINT [CK_dv_sat__hash] CHECK ([hashmatching_type]='None' OR [hashmatching_type]='MD5' OR [hashmatching_type]='SHA1'),
    CONSTRAINT [FK__dv_satellite__dv_hub] FOREIGN KEY ([hub_key]) REFERENCES [dbo].[dv_hub] ([hub_key]),
    CONSTRAINT [FK__dv_satellite__dv_link] FOREIGN KEY ([link_key]) REFERENCES [dbo].[dv_link] ([link_key]),
    CONSTRAINT [FK_dv_satellite_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_sat_abr_unique] UNIQUE NONCLUSTERED ([satellite_abbreviation] ASC),
    CONSTRAINT [dv_satellite_unique] UNIQUE NONCLUSTERED ([satellite_name] ASC)
);




GO
CREATE trigger dbo.dv_satellite_audit
on [dbo].[dv_satellite]
after insert, update
as
begin
update a
set [updated_datetime] = sysdatetimeoffset(),
    [updated_by]	   = suser_name()
from [dbo].[dv_satellite] as a
join inserted as b 
on a.[satellite_key] = b.[satellite_key]; 
end