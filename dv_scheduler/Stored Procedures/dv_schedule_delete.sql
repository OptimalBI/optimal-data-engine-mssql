
CREATE PROC [dv_scheduler].[dv_schedule_delete] 
    @schedule_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dv_scheduler].[dv_schedule_delete]
	WHERE  [schedule_key] = @schedule_key

	COMMIT