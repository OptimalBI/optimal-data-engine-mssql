CREATE SERVICE [dv_scheduler_s002]
    AUTHORIZATION [dbo]
    ON QUEUE [dbo].[dv_scheduler_q002]
    ([dv_scheduler_c002]);