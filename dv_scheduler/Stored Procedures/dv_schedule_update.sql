CREATE PROC [dv_scheduler].[dv_schedule_update] 
    @schedule_key			int,
	@schedule_name			varchar(128),	
    @schedule_description	varchar(256),	
    @schedule_frequency		varchar(128),	
	@is_cancelled				bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dv_scheduler].[dv_schedule]
    SET [schedule_name]				= @schedule_name			
       ,[schedule_description]		= @schedule_description	
       ,[schedule_frequency]		= @schedule_frequency	
       ,[is_cancelled]				= @is_cancelled			
	WHERE [schedule_key]			= @schedule_key
	
	-- Begin Return Select <- do not remove
	SELECT [schedule_key]
      ,[schedule_name]
      ,[schedule_description]
      ,[schedule_frequency]
      ,[is_cancelled]
      ,[release_key]
      ,[version_number]
      ,[updated_by]
      ,[updated_datetime]
    FROM [dv_scheduler].[dv_schedule]
	WHERE [schedule_key] = @schedule_key
	-- End Return Select <- do not remove

	COMMIT