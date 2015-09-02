CREATE PROC [dbo].[dv_hub_key_delete] 
    @hub_key_column_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_hub_key_column]
	WHERE  [hub_key_column_key] = @hub_key_column_key

	COMMIT