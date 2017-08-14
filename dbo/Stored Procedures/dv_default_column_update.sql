CREATE PROC [dbo].[dv_default_column_update] 
    @default_column_key int,
    @object_type varchar(30),
    @object_column_type varchar(30),
    @ordinal_position int,
    @column_prefix varchar(30) = NULL,
    @column_name varchar(256),
    @column_suffix varchar(30) = NULL,
    @column_type varchar(30),
    @column_length int = NULL,
    @column_precision int = NULL,
    @column_scale int = NULL,
    @collation_Name nvarchar(128) = NULL,
    @is_nullable bit,
    @is_pk bit,
    @discard_flag bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_default_column]
	SET    [object_type] = @object_type
	, [object_column_type] = @object_column_type
	, [ordinal_position] = @ordinal_position
	, [column_prefix] = @column_prefix
	, [column_name] = @column_name
	, [column_suffix] = @column_suffix
	, [column_type] = @column_type
	, [column_length] = @column_length
	, [column_precision] = @column_precision
	, [column_scale] = @column_scale
	, [collation_Name] = @collation_Name
	, [is_nullable] = @is_nullable
	, [is_pk] = @is_pk
	, [discard_flag] = @discard_flag
	WHERE  [default_column_key] = @default_column_key
	
	-- Begin Return Select <- do not remove
	SELECT [default_column_key], [object_type], [object_column_type], [ordinal_position], [column_prefix], [column_name], [column_suffix], [column_type], [column_length], [column_precision], [column_scale], [collation_Name], [is_nullable], [is_pk], [discard_flag], [version_number], [updated_by], [update_date_time]
	FROM   [dbo].[dv_default_column]
	WHERE  [default_column_key] = @default_column_key	
	-- End Return Select <- do not remove

	COMMIT