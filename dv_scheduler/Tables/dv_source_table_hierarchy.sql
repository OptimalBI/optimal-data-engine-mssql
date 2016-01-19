CREATE TABLE [dv_scheduler].[dv_source_table_hierarchy] (
    [source_table_hierarchy_key] INT                IDENTITY (1, 1) NOT NULL,
    [source_table_key]           INT                NOT NULL,
    [prior_table_key]            INT                NOT NULL,
    [is_cancelled]               BIT                CONSTRAINT [DF_dv_source_table_hierarchy_is_deleted] DEFAULT ((0)) NOT NULL,
    [release_key]                INT                CONSTRAINT [DF_dv_source_table_hierarchy_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]             INT                CONSTRAINT [DF_dv_source_table_hierarchy_version_number] DEFAULT ((1)) NOT NULL,
    [updated_by]                 VARCHAR (30)       CONSTRAINT [DF_dv_source_table_hierarchy_updated_by] DEFAULT (user_name()) NOT NULL,
    [update_date_time]           DATETIMEOFFSET (7) CONSTRAINT [DF_dv_source_table_hierarchy_update_date_time] DEFAULT (sysdatetimeoffset()) NOT NULL,
    CONSTRAINT [PK__dv_sourc__83E05932811DCE8D] PRIMARY KEY CLUSTERED ([source_table_hierarchy_key] ASC),
    CONSTRAINT [FK_dv_source_table_hierarchy__prior_source_table] FOREIGN KEY ([prior_table_key]) REFERENCES [dbo].[dv_source_table] ([source_table_key]),
    CONSTRAINT [FK_dv_source_table_hierarchy__source_table] FOREIGN KEY ([source_table_key]) REFERENCES [dbo].[dv_source_table] ([source_table_key]),
    CONSTRAINT [FK_dv_source_table_hierarchy_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key])
);


GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_Source_table_key_prior_table_key]
    ON [dv_scheduler].[dv_source_table_hierarchy]([source_table_key] ASC, [prior_table_key] ASC);

