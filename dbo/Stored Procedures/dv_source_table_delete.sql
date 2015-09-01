CREATE PROC [dbo].[dv_source_table_delete] 
    @table_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_source_table]
	WHERE  [table_key] = @table_key

	COMMIT
