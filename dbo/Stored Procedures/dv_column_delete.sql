CREATE PROC [dbo].[dv_column_delete] 
    @column_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_column]
	WHERE  [column_key] = @column_key

	COMMIT