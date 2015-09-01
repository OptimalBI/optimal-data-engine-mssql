CREATE PROC [dbo].[dv_defaults_delete] 
    @default_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_defaults]
	WHERE  [default_key] = @default_key

	COMMIT
