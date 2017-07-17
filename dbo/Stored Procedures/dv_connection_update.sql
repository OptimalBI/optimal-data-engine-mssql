

CREATE PROC [dbo].[dv_connection_update] 
    @connection_key      int,
	@connection_name     varchar(128),   
    @connection_string	 varchar(256),           
    @connection_password varchar(50)

AS 
SET NOCOUNT ON 
SET XACT_ABORT ON 	
BEGIN TRAN

    UPDATE [dbo].[dv_connection]
    SET [connection_name]		= @connection_name
       ,[connection_string]		= @connection_string	
       ,[connection_password]	= @connection_password			
 	WHERE [connection_key]		= @connection_key
	
	-- Begin Return Select <- do not remove
SELECT *
  FROM [dbo].[dv_connection]
  WHERE  [connection_key] = @connection_key
	-- End Return Select <- do not remove
COMMIT