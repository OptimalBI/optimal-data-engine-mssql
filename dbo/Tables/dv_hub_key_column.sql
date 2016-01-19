CREATE TABLE [dbo].[dv_hub_key_column] (
    [hub_key_column_key]       INT                IDENTITY (1, 1) NOT NULL,
    [hub_key]                  INT                NOT NULL,
    [hub_key_column_name]      VARCHAR (128)      NOT NULL,
    [hub_key_column_type]      VARCHAR (30)       NOT NULL,
    [hub_key_column_length]    INT                NULL,
    [hub_key_column_precision] INT                NULL,
    [hub_key_column_scale]     INT                NULL,
    [hub_key_Collation_Name]   [sysname]          NULL,
    [hub_key_ordinal_position] INT                CONSTRAINT [DF__dv_hub_ke__hub_k__308E3499] DEFAULT ((0)) NOT NULL,
    [release_key]              INT                CONSTRAINT [DF_dv_hub_key_column_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]           INT                CONSTRAINT [DF__dv_hub_ke__versi__318258D2] DEFAULT ((1)) NOT NULL,
    [updated_by]               VARCHAR (30)       CONSTRAINT [DF__dv_hub_ke__updat__32767D0B] DEFAULT (user_name()) NULL,
    [updated_datetime]         DATETIMEOFFSET (7) CONSTRAINT [DF__dv_hub_ke__updat__336AA144] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_hub_k__E124E9D92355792F] PRIMARY KEY CLUSTERED ([hub_key_column_key] ASC),
    CONSTRAINT [FK__dv_hub_key_column__dv_hub] FOREIGN KEY ([hub_key]) REFERENCES [dbo].[dv_hub] ([hub_key]),
    CONSTRAINT [FK_dv_hub_key_column_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_hub_column_key_unique] UNIQUE NONCLUSTERED ([hub_key] ASC, [hub_key_column_name] ASC)
);

