
CREATE PROC [dbo].[dv_connection_delete] 
    @connection_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_connection]
	WHERE  [connection_key] = @connection_key

	COMMIT