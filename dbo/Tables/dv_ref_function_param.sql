CREATE TABLE [dbo].[dv_ref_function_param] (
    [ref_function_param_key] INT                IDENTITY (1, 1) NOT NULL,
    [ref_function_key]       INT                NOT NULL,
    [param_name]             VARCHAR (128)      NOT NULL,
    [param_type]             VARCHAR (30)       NOT NULL,
    [param_length]           INT                NULL,
    [param_precision]        INT                NULL,
    [param_scale]            INT                NULL,
    [collation_name]         [sysname]          NULL,
    [default_value]          VARCHAR (512)      NULL,
    [param_ordinal_position] INT                CONSTRAINT [DF_ref_function_param_param_ordinal_position] DEFAULT ((0)) NOT NULL,
    [is_retired]             BIT                CONSTRAINT [DF_ref_function_param_] DEFAULT ((0)) NOT NULL,
    [release_key]            INT                CONSTRAINT [DF_ref_function_param_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]         INT                CONSTRAINT [DF_ref_function_param_version_number] DEFAULT ((1)) NOT NULL,
    [updated_by]             VARCHAR (30)       CONSTRAINT [DF_ref_function_param_updated_by] DEFAULT (suser_name()) NULL,
    [update_date_time]       DATETIMEOFFSET (7) CONSTRAINT [DF_ref_function_param_update_date_time] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_ref_function_param] PRIMARY KEY CLUSTERED ([ref_function_param_key] ASC),
    CONSTRAINT [FK__dv_ref_function] FOREIGN KEY ([ref_function_key]) REFERENCES [dbo].[dv_ref_function] ([ref_function_key]),
    CONSTRAINT [dv_ref_function_param_unique] UNIQUE NONCLUSTERED ([param_name] ASC)
);

