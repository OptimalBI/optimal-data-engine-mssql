CREATE TABLE [dv_log].[dv_load_state] (
    [load_state_key]        INT                IDENTITY (1, 1) NOT NULL,
    [source_table_key]      INT                DEFAULT ((-1)) NULL,
    [object_key]            INT                DEFAULT ((-1)) NULL,
    [object_type]           VARCHAR (50)       DEFAULT ('<Unknown>') NULL,
    [execution_key]         INT                DEFAULT ((-1)) NULL,
    [run_key]               INT                NULL,
    [load_high_water]       DATETIMEOFFSET (7) NULL,
    [lookup_start_datetime] DATETIMEOFFSET (7) NULL,
    [load_start_datetime]   DATETIMEOFFSET (7) NULL,
    [load_end_datetime]     DATETIMEOFFSET (7) NULL,
    [rows_inserted]         INT                DEFAULT ((0)) NULL,
    [rows_updated]          INT                DEFAULT ((0)) NULL,
    [rows_deleted]          INT                DEFAULT ((0)) NULL,
    [rows_affected]         INT                DEFAULT ((0)) NULL,
    [updated_by]            VARCHAR (30)       DEFAULT (suser_name()) NULL,
    [update_date_time]      DATETIMEOFFSET (7) DEFAULT (sysdatetimeoffset()) NULL,
    PRIMARY KEY CLUSTERED ([load_state_key] ASC)
);




GO
