CREATE PROC [dbo].[dv_hub_column_delete] 
    @hub_col_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_hub_column]
	WHERE  [hub_col_key] = @hub_col_key

	COMMIT
