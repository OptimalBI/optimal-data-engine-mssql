

CREATE PROC [dv_scheduler].[dv_source_table_hierarchy_update] 
    @source_table_hierarchy_key int,
    @source_table_key int,
    @prior_table_key int,
	@is_cancelled bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dv_scheduler].[dv_source_table_hierarchy]
	SET    [source_table_key] = @source_table_key, [prior_table_key] = @prior_table_key, is_cancelled = @is_cancelled
	WHERE  [source_table_hierarchy_key] = @source_table_hierarchy_key
	
	-- Begin Return Select <- do not remove
	SELECT [source_table_hierarchy_key], [source_table_key], [prior_table_key], [is_cancelled]
	FROM   [dv_scheduler].[dv_source_table_hierarchy]
	WHERE  [source_table_hierarchy_key] = @source_table_hierarchy_key	
	-- End Return Select <- do not remove

	COMMIT