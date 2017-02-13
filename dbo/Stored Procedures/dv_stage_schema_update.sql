
CREATE PROC [dbo].[dv_stage_schema_update]
    @stage_schema_key int, 
    @stage_database_key int,
    @stage_schema_name varchar(128),
	@is_retired bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_stage_schema]
	SET [stage_database_key] = @stage_database_key, [stage_schema_name] = @stage_schema_name, [is_retired] = @is_retired
	WHERE  [stage_schema_key] = @stage_schema_key
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_stage_schema]
	WHERE  [stage_schema_key] = @stage_schema_key	
	-- End Return Select <- do not remove

	COMMIT