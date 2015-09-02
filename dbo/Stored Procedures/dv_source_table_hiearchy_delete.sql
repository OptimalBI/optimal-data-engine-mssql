CREATE PROC [dbo].[dv_source_table_hiearchy_delete] 
    @table_hiearchy_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_source_table_hiearchy]
	WHERE  [table_hiearchy_key] = @table_hiearchy_key

	COMMIT