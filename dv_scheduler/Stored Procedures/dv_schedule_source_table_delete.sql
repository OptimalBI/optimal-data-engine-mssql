
CREATE PROC [dv_scheduler].[dv_schedule_source_table_delete] 
    @schedule_source_table_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dv_scheduler].[dv_schedule_source_table]
	WHERE  [schedule_source_table_key] = @schedule_source_table_key	

	COMMIT