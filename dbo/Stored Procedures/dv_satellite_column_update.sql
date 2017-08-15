CREATE PROC [dbo].[dv_satellite_column_update] 
    @satellite_col_key int,
    @satellite_key int,
	@column_name [varchar](128),
	@column_type [varchar](30),
	@column_length [int],
	@column_precision [int],
	@column_scale [int],
	@Collation_Name [sysname],
	@satellite_ordinal_position [int],
	@ref_function_key [int],
    @func_arguments [nvarchar](512),
	@func_ordinal_position [int]
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_satellite_column]
	SET    [satellite_key] = @satellite_key
	, [column_name] = @column_name
	, [column_type] = @column_type
	, [column_length] = @column_length
	, [column_precision] = @column_precision
	, [column_scale] = @column_scale
	, [Collation_Name] = @Collation_Name
	, [func_arguments] = @func_arguments
	, [satellite_ordinal_position] = @satellite_ordinal_position
	, [ref_function_key] = @ref_function_key
	, [func_ordinal_position] = @func_ordinal_position
	WHERE  [satellite_col_key] = @satellite_col_key
	
	-- Begin Return Select <- do not remove
	SELECT [satellite_col_key], [satellite_key],[column_name],[column_type],[column_length],[column_precision],[column_scale],[Collation_Name],[satellite_ordinal_position],[ref_function_key],[func_arguments], [func_ordinal_position],[release_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_satellite_column]
	WHERE  [satellite_col_key] = @satellite_col_key	
	-- End Return Select <- do not remove

	COMMIT