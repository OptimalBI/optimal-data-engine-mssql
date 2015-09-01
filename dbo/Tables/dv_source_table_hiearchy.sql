CREATE TABLE [dbo].[dv_source_table_hiearchy] (
    [table_hiearchy_key] INT                IDENTITY (1, 1) NOT NULL,
    [table_key]          INT                NOT NULL,
    [prior_table_key]    INT                NOT NULL,
    [release_key]        INT                CONSTRAINT [DF_dv_source_table_hiearchy_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]     INT                CONSTRAINT [DF_dv_source_table_hiearchy_version_number] DEFAULT ((1)) NULL,
    [updated_by]         VARCHAR (30)       CONSTRAINT [DF_dv_source_table_hiearchy_updated_by] DEFAULT (user_name()) NULL,
    [update_date_time]   DATETIMEOFFSET (7) CONSTRAINT [DF_dv_source_table_hiearchy_update_date_time] DEFAULT (sysdatetimeoffset()) NULL,
    PRIMARY KEY CLUSTERED ([table_hiearchy_key] ASC),
    CONSTRAINT [FK_dv_source_table_hiearchy__prior_source_table] FOREIGN KEY ([prior_table_key]) REFERENCES [dbo].[dv_source_table] ([table_key]),
    CONSTRAINT [FK_dv_source_table_hiearchy__source_table] FOREIGN KEY ([table_key]) REFERENCES [dbo].[dv_source_table] ([table_key]),
    CONSTRAINT [FK_dv_source_table_hiearchy_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key])
);



