CREATE PROC [dv_scheduler].[dv_source_table_hierarchy_insert] 
     @source_unique_name		varchar(128)
    ,@prior_source_unique_name	varchar(128)
	,@release_number			int
AS
BEGIN 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN
	
	declare @release_key						int
	       ,@source_table_key					int
		   ,@prior_source_table_key				int
	       ,@rc									int
	select @release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @release_number
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Release Number %i Does Not Exist', 16, 1, @release_number)

	select @source_table_key = [source_table_key] 
	from [dbo].[dv_source_table] st
	where st.[source_unique_name]		= @source_unique_name
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Source Table %s Does Not Exist', 16, 1, @source_unique_name)

	select @prior_source_table_key = [source_table_key] 
	from [dbo].[dv_source_table] st
	where st.[source_unique_name]		= @prior_source_unique_name
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Prior Source Table %s Does Not Exist', 16, 1, @prior_source_unique_name)

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