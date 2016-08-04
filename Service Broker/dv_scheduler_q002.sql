CREATE QUEUE [dbo].[dv_scheduler_q002]
    WITH ACTIVATION (STATUS = ON, PROCEDURE_NAME = [dv_scheduler].[dv_process_queued_002], MAX_QUEUE_READERS = 3, EXECUTE AS N'SBLogin');

