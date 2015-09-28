CREATE PROC [dbo].[dv_source_system_update] 
    @system_key int,
    @source_system_name varchar(50),
    @timevault_name varchar(50)
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_source_system]
	SET    [source_system_name] = @source_system_name, [timevault_name] = @timevault_name
	WHERE  [source_system_key] = @system_key
	
	-- Begin Return Select <- do not remove
	SELECT [source_system_key], [source_system_name], [timevault_name]
	FROM   [dbo].[dv_source_system]
	WHERE  [source_system_key] = @system_key	
	-- End Return Select <- do not remove

	COMMIT