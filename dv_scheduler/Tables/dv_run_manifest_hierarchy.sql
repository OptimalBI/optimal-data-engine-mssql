CREATE TABLE [dv_scheduler].[dv_run_manifest_hierarchy] (
    [run_manifest_hierarchy_key] INT                IDENTITY (1, 1) NOT NULL,
    [run_manifest_key]           INT                NOT NULL,
    [run_manifest_prior_key]     INT                NOT NULL,
    [update_date_time]           DATETIMEOFFSET (7) CONSTRAINT [DF_dv_run_manifest_hierarchy_update_date_time] DEFAULT (sysdatetimeoffset()) NULL,
    PRIMARY KEY CLUSTERED ([run_manifest_hierarchy_key] ASC),
    CONSTRAINT [FK_dv_run_manifest_hierarchy__manifest_key] FOREIGN KEY ([run_manifest_key]) REFERENCES [dv_scheduler].[dv_run_manifest] ([run_manifest_key]),
    CONSTRAINT [FK_dv_run_manifest_hierarchy__manifest_prior_key] FOREIGN KEY ([run_manifest_prior_key]) REFERENCES [dv_scheduler].[dv_run_manifest] ([run_manifest_key])
);
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_run_manifest_key__run_manifest_prior_key]
    ON [dv_scheduler].[dv_run_manifest_hierarchy]([run_manifest_key] ASC, [run_manifest_prior_key] ASC);