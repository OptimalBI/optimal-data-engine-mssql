
CREATE PROC [dbo].[dv_source_system_update] 
    @system_key				int,
    @source_system_name		varchar(50),
	@source_database_name	varchar(50),
    @package_folder			varchar(256), 
    @package_project 		varchar(256),
    @project_connection_name varchar(50),
	@is_retired				bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_source_system]
	SET    [source_system_name]		= @source_system_name
	     , [source_database_name]	= @source_database_name
	     , [package_folder]			= @package_folder
	     , [package_project]		= @package_project
	     , [project_connection_name] = @project_connection_name
	     , [is_retired]				= @is_retired
	WHERE  [source_system_key]		= @system_key
	
	-- Begin Return Select <- do not remove
	SELECT * FROM [dbo].[dv_source_system]
	WHERE  [source_system_key] = @system_key	
	-- End Return Select <- do not remove

	COMMIT