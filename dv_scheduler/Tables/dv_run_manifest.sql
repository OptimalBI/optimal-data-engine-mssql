CREATE TABLE [dv_scheduler].[dv_run_manifest] (
    [run_manifest_key]       INT                IDENTITY (1, 1) NOT NULL,
    [run_key]                INT                NOT NULL,
    [source_unique_name]     VARCHAR (128)      NOT NULL,
    [source_table_load_type] VARCHAR (50)       NOT NULL,
    [source_table_key]       INT                NOT NULL,
    [priority]               VARCHAR (10)       NOT NULL,
    [queue]                  VARCHAR (10)       CONSTRAINT [DF_dv_run_manifest_queue] DEFAULT ('002') NOT NULL,
    [start_datetime]         DATETIMEOFFSET (7) NULL,
    [completed_datetime]     DATETIMEOFFSET (7) NULL,
    [run_status]             VARCHAR (128)      CONSTRAINT [DF_dv_run_manifest_run_status] DEFAULT ('Scheduled') NOT NULL,
    [row_count]              INT                CONSTRAINT [DF_dv_run_manifest_row_count] DEFAULT ((0)) NOT NULL,
    [session_id]             INT                NULL,
    CONSTRAINT [PK__dv_run_m__C9D207B6B86E4AF4] PRIMARY KEY CLUSTERED ([run_manifest_key] ASC),
    CONSTRAINT [CK_dv_run_manifest__priority] CHECK ([priority]='high' OR [priority]='low'),
    CONSTRAINT [CK_dv_run_manifest__queue] CHECK ([queue]='001' OR [queue]='002'),
    CONSTRAINT [CK_dv_run_manifest__run_status] CHECK ([run_status]='Scheduled' OR [run_status]='Queued' OR [run_status]='Processing' OR [run_status]='Completed' OR [run_status]='Cancelled' OR [run_status]='Failed'),
    CONSTRAINT [CK_dv_run_manifest__run_type] CHECK ([source_table_load_type]='Full' OR [source_table_load_type]='Delta'),
    CONSTRAINT [FK_dv_run_manifest__dv_run] FOREIGN KEY ([run_key]) REFERENCES [dv_scheduler].[dv_run] ([run_key])
);




GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_dv_run_manifest__run_key_source_table]
    ON [dv_scheduler].[dv_run_manifest]([run_key] ASC, [source_unique_name] ASC);




GO
CREATE Trigger [dv_scheduler].[trg_dv_manifest_status] on [dv_scheduler].[dv_run_manifest]
AFTER UPDATE AS
BEGIN
  /* Insert Can Only be Status 'Scheduled'*/
BEGIN TRY
IF EXISTS (SELECT 1 FROM inserted i
            inner join deleted d
			on i.[run_manifest_key] = d.[run_manifest_key]
			WHERE ((i.[run_status] = 'Queued'		and d.[run_status] <> 'Scheduled' )	or
			       (i.[run_status] = 'Processing'	and d.[run_status] <> 'Queued'	  ) or
				   (i.[run_status] = 'Completed'	and d.[run_status] <> 'Processing')	or
				   (i.[run_status] = 'Failed'		and d.[run_status] <> 'Processing') 			
				   )
			)
      THROW 50000, N'Invalid Status Change Detected', 1;
END TRY
BEGIN CATCH
	IF (@@TRANCOUNT > 0)
		ROLLBACK;
		THROW; 
END CATCH;
END;