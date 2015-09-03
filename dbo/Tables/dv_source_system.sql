CREATE TABLE [dbo].[dv_source_system] (
    [source_system_key]         INT                IDENTITY (1, 1) NOT NULL,
    [source_system_name] VARCHAR (50)       NOT NULL,
    [timevault_name]     VARCHAR (50)       NULL,
    [release_key]        INT                CONSTRAINT [DF_dv_source_system_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]     INT                CONSTRAINT [DF__dv_source__versi__02084FDA] DEFAULT ((1)) NULL,
    [updated_by]         VARCHAR (30)       CONSTRAINT [DF__dv_source__updat__02FC7413] DEFAULT (user_name()) NULL,
    [update_date_time]   DATETIMEOFFSET (7) CONSTRAINT [DF__dv_source__updat__03F0984C] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_sourc__B5998963B2793DE4] PRIMARY KEY CLUSTERED ([source_system_key] ASC),
    CONSTRAINT [FK_dv_source_system_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [source_system_unique] UNIQUE NONCLUSTERED ([source_system_name] ASC)
);