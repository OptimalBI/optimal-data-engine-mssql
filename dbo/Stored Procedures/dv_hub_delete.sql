CREATE PROC [dbo].[dv_hub_delete] 
    @hub_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_hub]
	WHERE  [hub_key] = @hub_key

	COMMIT