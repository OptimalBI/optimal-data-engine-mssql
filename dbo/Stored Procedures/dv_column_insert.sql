CREATE PROC [dbo].[dv_column_insert] 
    @table_key int,
	@release_number int,
    @column_name varchar(128),
    @column_type varchar(30),
    @column_length int = NULL,
    @column_precision int = NULL,
    @column_scale int = NULL,
    @Collation_Name nvarchar(128) = NULL,
    @bk_ordinal_position int,
    @source_ordinal_position int,
    @satellite_ordinal_position int,
    @is_source_date bit,
    @discard_flag bit,
    @deleted_column_flag bit
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
	INSERT INTO [dbo].[dv_column] ([table_key], [column_name], [column_type], [column_length], [column_precision], [column_scale], [Collation_Name], [bk_ordinal_position], [source_ordinal_position], [satellite_ordinal_position], [is_source_date], [discard_flag], [deleted_column_flag],[release_key])
	SELECT @table_key, @column_name, @column_type, @column_length, @column_precision, @column_scale, @Collation_Name, @bk_ordinal_position, @source_ordinal_position, @satellite_ordinal_position, @is_source_date, @discard_flag, @deleted_column_flag, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [column_key], [table_key], [column_name], [column_type], [column_length], [column_precision], [column_scale], [Collation_Name], [bk_ordinal_position], [source_ordinal_position], [satellite_ordinal_position], [is_source_date], [discard_flag], [deleted_column_flag],[release_key],[version_number], [updated_by], [update_date_time]
	FROM   [dbo].[dv_column]
	WHERE  [column_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()

