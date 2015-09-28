CREATE TABLE [dv_release].[dv_release_master] (
    [release_key]               INT                IDENTITY (1, 1) NOT NULL,
    [release_number]            INT                NOT NULL,
    [release_description]       VARCHAR (256)      NULL,
    [reference_number]          VARCHAR (50)       NULL,
    [reference_source]          VARCHAR (50)       NULL,
    [build_number]              INT                CONSTRAINT [DF_release.dv_release_master_build_number] DEFAULT ((0)) NOT NULL,
    [build_date]                DATETIMEOFFSET (7) NULL,
    [build_server]              VARCHAR (256)      NULL,
    [release_built_by]          VARCHAR (30)       NULL,
    [release_start_datetime]    DATETIMEOFFSET (7) NULL,
    [release_complete_datetime] DATETIMEOFFSET (7) NULL,
    [release_count]             INT                CONSTRAINT [DF_release.dv_release_master_release_count] DEFAULT ((0)) NOT NULL,
    [version_number]            INT                CONSTRAINT [DF_release.dv_release_master_version_number] DEFAULT ((1)) NOT NULL,
    [updated_by]                VARCHAR (30)       CONSTRAINT [DF_release.dv_release_master_updated_by] DEFAULT (user_name()) NOT NULL,
    [updated_datetime]          DATETIMEOFFSET (7) CONSTRAINT [DF_release.dv_release_master_updated_datetime] DEFAULT (sysdatetimeoffset()) NOT NULL,
    CONSTRAINT [PK__dv_relea__7B7C0773AC625D81] PRIMARY KEY CLUSTERED ([release_key] ASC)
);
GO
CREATE UNIQUE NONCLUSTERED INDEX [dv_release_number]
    ON [dv_release].[dv_release_master]([release_number] ASC);