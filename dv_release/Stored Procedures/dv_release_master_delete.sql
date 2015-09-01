CREATE PROC [dv_release].[dv_release_master_delete] 
    @release_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dv_release].[dv_release_master]
	WHERE  [release_key] = @release_key

	COMMIT