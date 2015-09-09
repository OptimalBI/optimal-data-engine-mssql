CREATE TABLE [dv_scheduler].[dv_run] (
    [run_key]            INT                IDENTITY (1, 1) NOT NULL,
    [run_status]         VARCHAR (128)      CONSTRAINT [DF__dv_run__run_stat__39237A9A] DEFAULT ('Scheduled') NOT NULL,
    [run_schedule_name]  VARCHAR (128)      NOT NULL,
    [run_start_datetime] DATETIMEOFFSET (7) NULL,
    [run_end_datetime]   DATETIMEOFFSET (7) NULL,
    [updated_datetime]   DATETIMEOFFSET (7) CONSTRAINT [DF__dv_run__updated___3A179ED3] DEFAULT (sysdatetimeoffset()) NOT NULL,
    CONSTRAINT [PK__dv_run__AEDC1D6EF837117B] PRIMARY KEY CLUSTERED ([run_key] ASC)
);

