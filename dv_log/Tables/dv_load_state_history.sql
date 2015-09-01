CREATE TABLE [dv_log].[dv_load_state_history] (
    [load_state_key]   INT                NULL,
    [activity]         CHAR (1)           NULL,
    [source_table_key] INT                NULL,
    [object_key]       INT                NULL,
    [object_type]      VARCHAR (50)       NULL,
    [execution_key]    INT                NULL,
    [load_high_water]  DATETIMEOFFSET (7) NULL,
    [rows_inserted]    INT                NULL,
    [rows_updated]     INT                NULL,
    [rows_deleted]     INT                NULL,
    [updated_by]       VARCHAR (30)       NULL,
    [update_date_time] DATETIMEOFFSET (7) NULL
);

