CREATE TABLE [dv_log].[dv_load_state_history] (
    [load_state_history_key] INT                IDENTITY (1, 1) NOT NULL,
    [load_state_key]         INT                NOT NULL,
    [activity]               CHAR (1)           NULL,
    [source_table_key]       INT                NULL,
    [object_key]             INT                NULL,
    [object_type]            VARCHAR (50)       NULL,
    [execution_key]          INT                NULL,
    [run_key]                INT                NULL,
    [load_high_water]        DATETIMEOFFSET (7) NULL,
    [lookup_start_datetime]  DATETIMEOFFSET (7) NULL,
    [load_start_datetime]    DATETIMEOFFSET (7) NULL,
    [load_end_datetime]      DATETIMEOFFSET (7) NULL,
    [rows_inserted]          INT                NULL,
    [rows_updated]           INT                NULL,
    [rows_deleted]           INT                NULL,
    [rows_affected]          INT                NULL,
    [updated_by]             VARCHAR (30)       NULL,
    [update_date_time]       DATETIMEOFFSET (7) NULL,
    PRIMARY KEY CLUSTERED ([load_state_history_key] ASC)
);



