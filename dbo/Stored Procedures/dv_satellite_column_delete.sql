CREATE PROC [dbo].[dv_satellite_column_delete] 
    @satellite_col_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_satellite_column]
	WHERE  [satellite_col_key] = @satellite_col_key

	COMMIT
