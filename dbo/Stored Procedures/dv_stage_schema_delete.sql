

CREATE PROC [dbo].[dv_stage_schema_delete] 
    @stage_schema_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_stage_schema]
	WHERE  [stage_schema_key] = @stage_schema_key

	COMMIT