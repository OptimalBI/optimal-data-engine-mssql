CREATE TABLE [dv_scheduler].[dv_run_manifest] (
    [run_manifest_key]       INT                IDENTITY (1, 1) NOT NULL,
    [run_key]                INT                NOT NULL,
    [source_system_name]     [sysname]          NOT NULL,
    [source_table_schema]    [sysname]          NOT NULL,
    [source_table_name]      [sysname]          NOT NULL,
    [source_table_load_type] VARCHAR (50)       NOT NULL,
    [source_procedure_name]  VARCHAR (128)      NULL,
    [priority]               VARCHAR (10)       NOT NULL,
    [queue]                  VARCHAR (10)       NOT NULL,
    [start_datetime]         DATETIMEOFFSET (7) NULL,
    [completed_datetime]     DATETIMEOFFSET (7) NULL,
    [run_status]             VARCHAR (128)      CONSTRAINT [DF_dv_run_manifest_run_status] DEFAULT ('Scheduled') NOT NULL,
    [row_count]              INT                CONSTRAINT [DF_dv_run_manifest_row_count] DEFAULT ((0)) NOT NULL,
    [session_id]             INT                NULL,
    CONSTRAINT [PK__dv_run_m__C9D207B6B86E4AF4] PRIMARY KEY CLUSTERED ([run_manifest_key] ASC)
);

