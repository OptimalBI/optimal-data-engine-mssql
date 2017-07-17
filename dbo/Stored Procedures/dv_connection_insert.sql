
CREATE PROC [dbo].[dv_connection_insert] 
    @connection_name     varchar(128),   
    @connection_string	 varchar(256),           
    @connection_password varchar(50)
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN
-- Note this table does not form part of the release process.
-- It is designed to be different in each environment.
	
	--declare @release_key int
	--       ,@rc int
	--select @release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @release_number
	--set @rc = @@rowcount
	--if @rc <> 1 
	--	RAISERROR('Release Number %i Does Not Exist', 16, 1, @release_number)

INSERT INTO [dbo].[dv_connection]([connection_name],[connection_string],[connection_password])
SELECT @connection_name,@connection_string,@connection_password
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_connection]
	WHERE  [connection_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()