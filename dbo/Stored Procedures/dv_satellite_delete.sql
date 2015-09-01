CREATE PROC [dbo].[dv_satellite_delete] 
    @satellite_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_satellite]
	WHERE  [satellite_key] = @satellite_key

	COMMIT
