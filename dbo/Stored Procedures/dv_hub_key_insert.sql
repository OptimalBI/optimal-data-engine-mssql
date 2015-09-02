CREATE PROC [dbo].[dv_hub_key_insert] 
    @hub_key int,
    @hub_key_column_name varchar(128),
    @hub_key_column_type varchar(30),
    @hub_key_column_length int = NULL,
    @hub_key_column_precision int = NULL,
    @hub_key_column_scale int = NULL,
    @hub_key_Collation_Name nvarchar(128) = NULL,
    @hub_key_ordinal_position int,
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

	INSERT INTO [dbo].[dv_hub_key_column] ([hub_key], [hub_key_column_name], [hub_key_column_type], [hub_key_column_length], [hub_key_column_precision], [hub_key_column_scale], [hub_key_Collation_Name], [hub_key_ordinal_position],[release_key])
	SELECT @hub_key, @hub_key_column_name, @hub_key_column_type, @hub_key_column_length, @hub_key_column_precision, @hub_key_column_scale, @hub_key_Collation_Name, @hub_key_ordinal_position, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [hub_key_column_key], [hub_key], [hub_key_column_name], [hub_key_column_type], [hub_key_column_length], [hub_key_column_precision], [hub_key_column_scale], [hub_key_Collation_Name], [hub_key_ordinal_position],[release_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_hub_key_column]
	WHERE  [hub_key_column_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()