
CREATE PROC [dbo].[dv_stage_database_update]
    @stage_database_key		int, 
    @stage_database_name	varchar(128),
	@stage_connection_name		varchar(512),
	@is_retired				bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_stage_database]
	SET    [stage_database_name]	= @stage_database_name
	     , [stage_connection_name]	= @stage_connection_name
		 , [is_retired]				= @is_retired
	WHERE  [stage_database_key]		= @stage_database_key
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_stage_database]
	WHERE  [stage_database_key] = @stage_database_key	
	-- End Return Select <- do not remove

	COMMIT