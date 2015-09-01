CREATE PROC [dbo].[dv_source_table_hiearchy_update] 
    @table_hiearchy_key int,
    @table_key int,
    @prior_table_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_source_table_hiearchy]
	SET    [table_key] = @table_key, [prior_table_key] = @prior_table_key
	WHERE  [table_hiearchy_key] = @table_hiearchy_key
	
	-- Begin Return Select <- do not remove
	SELECT [table_hiearchy_key], [table_key], [prior_table_key]
	FROM   [dbo].[dv_source_table_hiearchy]
	WHERE  [table_hiearchy_key] = @table_hiearchy_key	
	-- End Return Select <- do not remove

	COMMIT
