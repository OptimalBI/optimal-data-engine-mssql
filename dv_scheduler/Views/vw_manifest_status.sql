CREATE view [dv_scheduler].[vw_manifest_status]
as
select
         [source_table_name]		= quotename(m.[source_system_name]) + '.' + quotename(m.[source_table_schema]) + '.' + quotename(m.[source_table_name])
		,[run_manifest_status]		= m.[run_status]
        ,m.[start_datetime]
        ,m.[completed_datetime]
        ,[task_duration]			= convert(time, dateadd(second, datediff(second, m.[start_datetime], m.[completed_datetime]), 0))
        ,m.[queue]
        ,m.[priority]
		,[load_type]				= m.[source_table_load_type]
        ,r.[run_schedule_name]
        ,r.[run_start_datetime]
        ,r.[run_end_datetime]
        ,[run_duration]				= convert(time, dateadd(second, datediff(second, r.[run_start_datetime], r.[run_end_datetime]), 0))
        ,r.[run_status]
        ,m.[session_id]
        ,r.[run_key]
from [dv_scheduler].[dv_run] r
inner join [dv_scheduler].[dv_run_manifest] m
on m.[run_key] = r.[run_key]