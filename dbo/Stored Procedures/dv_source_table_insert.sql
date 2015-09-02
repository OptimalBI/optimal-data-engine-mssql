CREATE PROC [dbo].[dv_source_table_insert] 
    @system_key int,
    @source_table_schema varchar(128),
    @source_table_name varchar(128),
    @source_table_load_type varchar(50),
	@release_number int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN
	
	declare @release_key int
	       ,@rc int
	select @release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @release_number
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Release Number %i Does Not Exist', 16, 1, @release_number)

	INSERT INTO [dbo].[dv_source_table] ([system_key], [source_table_schema], [source_table_name], [source_table_load_type],[release_key])
	SELECT @system_key, @source_table_schema, @source_table_name, @source_table_load_type, @release_key 
	
	-- Begin Return Select <- do not remove
	SELECT [table_key], [system_key], [source_table_schema], [source_table_name], [source_table_load_type],[release_key]
	FROM   [dbo].[dv_source_table]
	WHERE  [table_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()