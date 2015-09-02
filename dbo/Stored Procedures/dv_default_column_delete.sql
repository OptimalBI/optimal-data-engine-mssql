CREATE PROC [dbo].[dv_default_column_delete] 
    @default_column_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_default_column]
	WHERE  [default_column_key] = @default_column_key

	COMMIT