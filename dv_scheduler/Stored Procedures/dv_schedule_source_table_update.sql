
CREATE PROC [dv_scheduler].[dv_schedule_source_table_update] 
    @schedule_source_table_key		int,
    @schedule_key					int,
    @source_table_key				int,
	@source_table_load_type			varchar(50),
	@priority						varchar(50),
	@queue							varchar(50),
	@is_cancelled bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dv_scheduler].[dv_schedule_source_table]
    SET [schedule_key]				= @schedule_key
       ,[source_table_key]			= @source_table_key
       ,[source_table_load_type]	= @source_table_load_type
       ,[priority]					= @priority
       ,[queue]						= @queue
       ,[is_cancelled]				= @is_cancelled
 	WHERE  [schedule_source_table_key] = @schedule_source_table_key
	
	-- Begin Return Select <- do not remove
	SELECT [schedule_source_table_key]
      ,[schedule_key]
      ,[source_table_key]
      ,[source_table_load_type]
      ,[priority]
      ,[queue]
      ,[is_cancelled]
      ,[release_key]
      ,[version_number]
      ,[updated_by]
      ,[updated_datetime]
  FROM [dv_scheduler].[dv_schedule_source_table]
	WHERE  [schedule_source_table_key] = @schedule_source_table_key	
	-- End Return Select <- do not remove

	COMMIT