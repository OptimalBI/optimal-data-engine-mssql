CREATE PROC [dbo].[dv_hub_key_update] 
    @hub_key_column_key int,
    @hub_key int,
    @hub_key_column_name varchar(128),
    @hub_key_column_type varchar(30),
    @hub_key_column_length int = NULL,
    @hub_key_column_precision int = NULL,
    @hub_key_column_scale int = NULL,
    @hub_key_Collation_Name nvarchar(128) = NULL,
    @hub_key_ordinal_position int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_hub_key_column]
	SET    [hub_key] = @hub_key
	, [hub_key_column_name] = @hub_key_column_name
	, [hub_key_column_type] = @hub_key_column_type
	, [hub_key_column_length] = @hub_key_column_length
	, [hub_key_column_precision] = @hub_key_column_precision
	, [hub_key_column_scale] = @hub_key_column_scale
	, [hub_key_Collation_Name] = @hub_key_Collation_Name
	, [hub_key_ordinal_position] = @hub_key_ordinal_position 
	WHERE  [hub_key_column_key] = @hub_key_column_key
	
	-- Begin Return Select <- do not remove
	SELECT [hub_key_column_key], [hub_key], [hub_key_column_name], [hub_key_column_type], [hub_key_column_length], [hub_key_column_precision], [hub_key_column_scale], [hub_key_Collation_Name], [hub_key_ordinal_position], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_hub_key_column]
	WHERE  [hub_key_column_key] = @hub_key_column_key	
	-- End Return Select <- do not remove

	COMMIT