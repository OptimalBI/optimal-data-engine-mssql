

CREATE PROC [dbo].[dv_stage_database_delete] 
    @stage_database_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_stage_database]
	WHERE  [stage_database_key] = @stage_database_key

	COMMIT