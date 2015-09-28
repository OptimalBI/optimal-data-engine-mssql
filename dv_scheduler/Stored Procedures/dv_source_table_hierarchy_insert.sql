

CREATE PROC [dv_scheduler].[dv_source_table_hierarchy_insert] 
	 @source_system_name		varchar(50)
	,@source_table_schema		varchar(128)
    ,@source_table_name			varchar(128)
	,@prior_source_system_name	varchar(50)
	,@prior_source_table_schema	varchar(128)
	,@prior_source_table_name	varchar(128)
	,@release_number			int
AS
BEGIN 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN
	
	declare @release_key						int
	       ,@source_table_key					int
		   ,@prior_source_table_key				int
		   ,@source_table_qualified_name		varchar(512)
		   ,@prior_source_table_qualified_name	varchar(512)
	       ,@rc									int
	select @release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @release_number
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Release Number %i Does Not Exist', 16, 1, @release_number)

	select @source_table_qualified_name = quotename(@source_system_name) + '.' + quotename(@source_table_schema) + '.' + quotename(@source_table_name)
	select @source_table_key = [source_table_key] 
	from [dbo].[dv_source_system] s
	inner join [dbo].[dv_source_table] st
	on s.[source_system_key] = st.[system_key]
	where s.[source_system_name]	= @source_system_name
	  and st.[source_table_schema]	= @source_table_schema
	  and [source_table_name]		= @source_table_name
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Source Table %s Does Not Exist', 16, 1, @source_table_qualified_name)

	select @prior_source_table_qualified_name = quotename(@prior_source_system_name) + '.' + quotename(@prior_source_table_schema) + '.' + quotename(@prior_source_table_name)
	select @prior_source_table_key = [source_table_key] 
	from [dbo].[dv_source_system] s
	inner join [dbo].[dv_source_table] st
	on s.[source_system_key] = st.[system_key]
	where s.[source_system_name]	= @prior_source_system_name
	  and st.[source_table_schema]	= @prior_source_table_schema
	  and [source_table_name]		= @prior_source_table_name
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Prior Source Table %s Does Not Exist', 16, 1, @prior_source_table_qualified_name)

	INSERT INTO [dv_scheduler].[dv_source_table_hierarchy]([source_table_key] ,[prior_table_key],[release_key])
     VALUES (@source_table_key ,@prior_source_table_key,@release_key)

	-- Begin Return Select <- do not remove

	SELECT [source_table_hierarchy_key]
		  ,[source_table_key]
		  ,[prior_table_key]
		  ,[release_key]
		  ,[version_number]
		  ,[updated_by]
		  ,[update_date_time]
	  FROM [dv_scheduler].[dv_source_table_hierarchy]
 	WHERE  [source_table_hierarchy_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()
END