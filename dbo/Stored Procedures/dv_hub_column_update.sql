CREATE PROC [dbo].[dv_hub_column_update] 
    @hub_col_key int,
    @hub_key_column_key int,
    @column_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_hub_column]
	SET    [hub_key_column_key] = @hub_key_column_key, [column_key] = @column_key
	WHERE  [hub_col_key] = @hub_col_key
	
	-- Begin Return Select <- do not remove
	SELECT [hub_col_key], [hub_key_column_key], [column_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_hub_column]
	WHERE  [hub_col_key] = @hub_col_key	
	-- End Return Select <- do not remove

	COMMIT