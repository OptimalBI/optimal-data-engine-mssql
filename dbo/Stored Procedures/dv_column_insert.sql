CREATE PROC [dbo].[dv_column_insert] 
    @table_key int,
	@release_number int ,
    @satellite_col_key int NULL,
    @column_name varchar(128),
    @column_type varchar(30),
    @column_length int = NULL,
    @column_precision int = NULL,
    @column_scale int = NULL,
    @Collation_Name nvarchar(128) = NULL,
	@is_derived	bit = NULL,
	@derived_value varchar(50) = NULL,
    @source_ordinal_position int,
    @is_source_date bit,
    @is_retired bit
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
	INSERT INTO [dbo].[dv_column] ([table_key], [satellite_col_key], [column_name], [column_type], [column_length], [column_precision], [column_scale], [Collation_Name], [is_derived], [derived_value], [source_ordinal_position], [is_source_date], [is_retired],[release_key])
	SELECT @table_key, @satellite_col_key, @column_name, @column_type, @column_length, @column_precision, @column_scale, @Collation_Name, @is_derived, @derived_value, @source_ordinal_position, @is_source_date, @is_retired, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT * FROM [dbo].[dv_column]
	WHERE  [column_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()