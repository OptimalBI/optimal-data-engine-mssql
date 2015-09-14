
CREATE PROC [dv_scheduler].[dv_source_table_hiearchy_delete] 
    @source_table_hiearchy_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dv_scheduler].[dv_source_table_hiearchy]
	WHERE  [source_table_hiearchy_key] = @source_table_hiearchy_key

	COMMIT