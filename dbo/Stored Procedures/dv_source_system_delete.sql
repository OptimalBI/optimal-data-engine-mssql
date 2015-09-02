CREATE PROC [dbo].[dv_source_system_delete] 
    @system_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_source_system]
	WHERE  [system_key] = @system_key

	COMMIT