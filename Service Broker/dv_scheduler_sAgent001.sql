CREATE SERVICE [dv_scheduler_sAgent001]
    AUTHORIZATION [dbo]
    ON QUEUE [dbo].[dv_scheduler_qAgent001]
    ([dv_scheduler_cAgent001]);

