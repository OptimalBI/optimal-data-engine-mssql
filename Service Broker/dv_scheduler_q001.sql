CREATE QUEUE [dbo].[dv_scheduler_q001]
    WITH ACTIVATION (STATUS = ON, PROCEDURE_NAME = [dv_scheduler].[dv_process_queued_001], MAX_QUEUE_READERS = 4, EXECUTE AS N'SBLogin');

