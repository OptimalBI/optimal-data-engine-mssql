CREATE TABLE [dv_scheduler].[dv_run] (
    [run_key]            INT                IDENTITY (1, 1) NOT NULL,
    [run_status]         VARCHAR (128)      CONSTRAINT [DF__dv_run__run_status] DEFAULT ('Scheduled') NOT NULL,
    [run_schedule_name]  VARCHAR (128)      NOT NULL,
    [run_start_datetime] DATETIMEOFFSET (7) NULL,
    [run_end_datetime]   DATETIMEOFFSET (7) NULL,
    [updated_datetime]   DATETIMEOFFSET (7) CONSTRAINT [DF__dv_run__updated__datetime] DEFAULT (sysdatetimeoffset()) NOT NULL,
    CONSTRAINT [PK__dv_run__AEDC1D6EF837117B] PRIMARY KEY CLUSTERED ([run_key] ASC),
    CONSTRAINT [CK_dv_run_status] CHECK ([run_status]='Scheduled' OR [run_status]='Started' OR [run_status]='Completed' OR [run_status]='Disabled' OR [run_status]='Cancelled' OR [run_status]='Failed')
);

