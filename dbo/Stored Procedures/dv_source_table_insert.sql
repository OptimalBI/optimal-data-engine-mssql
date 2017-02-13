CREATE PROC [dbo].[dv_source_table_insert] 
    @source_unique_name     varchar(128),   
    @source_type			varchar(50),           
    @load_type              varchar(50),             
    @system_key				int,            
    @source_table_schema    varchar(128),    
    @source_table_name      varchar(128),    
    @stage_schema_key       int,	    
    @stage_table_name       varchar(128),		
	@is_retired				bit,
	@release_number			int
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

	INSERT INTO [dbo].[dv_source_table] ([source_unique_name],[source_type],[load_type],[system_key],[source_table_schma],[source_table_nme],[stage_schema_key],[stage_table_name],[is_retired],[release_key])
	SELECT @source_unique_name,@source_type,@load_type,@system_key,@source_table_schema,@source_table_name,@stage_schema_key,@stage_table_name,@is_retired,@release_key 
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_source_table]
	WHERE  [source_table_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()