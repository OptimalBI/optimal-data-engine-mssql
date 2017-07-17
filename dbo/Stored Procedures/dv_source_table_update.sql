CREATE PROC [dbo].[dv_source_table_update] 
    @source_table_key		int,
    @source_unique_name     varchar(128),   
    @load_type              varchar(50),             
    @system_key				int,            
    @source_table_schema    varchar(128),    
    @source_table_name      varchar(128),    
    @stage_schema_key       int,
	@is_columnstore			bit,
	@is_compressed			bit,	    
    @stage_table_name       varchar(128),		
	@is_retired				bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_source_table]
	SET    [source_unique_name] = @source_unique_name,[load_type] = @load_type,[system_key] = @system_key,[source_table_schma] = @source_table_schema,[source_table_nme] = @source_table_name,[stage_schema_key] = @stage_schema_key,[stage_table_name] = @stage_table_name, [is_columnstore] = @is_columnstore , [is_compressed] = @is_compressed ,[is_retired] = @is_retired
	WHERE  [source_table_key] = @source_table_key
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_source_table]
	WHERE  [source_table_key] = @source_table_key	
	-- End Return Select <- do not remove

	COMMIT