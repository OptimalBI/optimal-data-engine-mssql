
CREATE PROC [dbo].[dv_source_version_update] 
    @source_version_key		int,
    @source_table_key		int,
	@source_version			int,
	@source_type			varchar(50),
	@source_procedure_name	varchar(128),
	@source_filter          nvarchar(max),
	@pass_load_type_to_proc	bit,
	@is_current				bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_source_version]
	SET    [source_table_key] = @source_table_key
	, [source_version] = @source_version
	, [source_type] = @source_type
	,[source_procedure_name] = @source_procedure_name 
	, [source_filter] = @source_filter
	, [pass_load_type_to_proc] = @pass_load_type_to_proc
	, [is_current] = @is_current
	WHERE  [source_version_key] = @source_version_key
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_source_version]
	WHERE  [source_version_key] = @source_version_key	
	-- End Return Select <- do not remove

	COMMIT