

CREATE PROC [dbo].[dv_source_version_delete] 
    @source_version_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_source_version]
	WHERE  [source_version_key] = @source_version_key

	COMMIT