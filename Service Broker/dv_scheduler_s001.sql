CREATE SERVICE [dv_scheduler_s001]
    AUTHORIZATION [dbo]
    ON QUEUE [dbo].[dv_scheduler_q001]
    ([dv_scheduler_c001]);