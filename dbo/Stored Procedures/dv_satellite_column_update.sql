CREATE PROC [dbo].[dv_satellite_column_update] 
    @satellite_col_key int,
    @satellite_key int,
    @column_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_satellite_column]
	SET    [satellite_key] = @satellite_key, [column_key] = @column_key
	WHERE  [satellite_col_key] = @satellite_col_key
	
	-- Begin Return Select <- do not remove
	SELECT [satellite_col_key], [satellite_key], [column_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_satellite_column]
	WHERE  [satellite_col_key] = @satellite_col_key	
	-- End Return Select <- do not remove

	COMMIT
