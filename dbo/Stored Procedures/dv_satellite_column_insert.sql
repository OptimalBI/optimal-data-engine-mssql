CREATE PROC [dbo].[dv_satellite_column_insert] 
    @satellite_key int,
    @column_key int,
	@column_name [varchar](128),
	@column_type [varchar](30),
	@column_length [int],
	@column_precision [int],
	@column_scale [int],
	@Collation_Name [sysname],
	@satellite_ordinal_position [int],
	@ref_function_key [int],
	@func_ordinal_position [int],
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

	INSERT INTO [dbo].[dv_satellite_column]([satellite_key],[column_key],[column_name],[column_type],[column_length],[column_precision],[column_scale],[Collation_Name],[satellite_ordinal_position],[ref_function_key],[func_ordinal_position],[release_key])
	SELECT @satellite_key, @column_key, @column_name,@column_type,@column_length,@column_precision,@column_scale,@Collation_Name,@satellite_ordinal_position,@ref_function_key,@func_ordinal_position,@release_key

	
	-- Begin Return Select <- do not remove
	SELECT [satellite_col_key], [satellite_key],[column_key],[column_name],[column_type],[column_length],[column_precision],[column_scale],[Collation_Name],[satellite_ordinal_position],[ref_function_key],[func_ordinal_position],[release_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_satellite_column]
	WHERE  [satellite_col_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()