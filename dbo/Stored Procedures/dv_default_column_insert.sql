CREATE PROC [dbo].[dv_default_column_insert] 
    @object_type varchar(30),
	@release_number int,
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
	
	declare @release_key int
	       ,@rc int
	select @release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @release_number
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Release Number %i Does Not Exist', 16, 1, @release_number)
	INSERT INTO [dbo].[dv_default_column] ([object_type], [object_column_type], [ordinal_position], [column_prefix], [column_name], [column_suffix], [column_type], [column_length], [column_precision], [column_scale], [collation_Name], [is_nullable], [is_pk], [discard_flag],[release_key])
	SELECT @object_type, @object_column_type, @ordinal_position, @column_prefix, @column_name, @column_suffix, @column_type, @column_length, @column_precision, @column_scale, @collation_Name, @is_nullable, @is_pk, @discard_flag, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [default_column_key], [object_type], [object_column_type], [ordinal_position], [column_prefix], [column_name], [column_suffix], [column_type], [column_length], [column_precision], [column_scale], [collation_Name], [is_nullable], [is_pk], [discard_flag], [release_key], [version_number], [updated_by], [update_date_time]
	FROM   [dbo].[dv_default_column]
	WHERE  [default_column_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()