
CREATE FUNCTION [dv_release].[fn_GetWaitingSchedulerTasks]
(@run_key int
)
RETURNS TABLE 
AS
RETURN 
(
select  m.source_system_name	
		  ,m.source_table_schema	
		  ,m.source_table_name	
		  ,m.source_procedure_name
		  ,m.source_table_load_type	
		  ,m.[queue]
		  --,m.run_status

	from [dv_scheduler].[dv_run] r
	inner join [dv_scheduler].[dv_run_manifest] m
	on m.run_key = r.run_key
	left join [dv_scheduler].[dv_run_manifest_hierarchy] h
	on m.run_manifest_key = h.run_manifest_key
	left join [dv_scheduler].[dv_run_manifest] m1
	on m1.run_manifest_key = h.[run_manifest_prior_key]
	where 1=1
	and @run_key = @run_key
	and r.run_status = 'Scheduled'
	and m.run_status = 'Scheduled'
	and isnull(m1.run_status, 'Completed') = 'Completed'
except
select  m.source_system_name	
		  ,m.source_table_schema	
		  ,m.source_table_name	
		  ,m.source_procedure_name
		  ,m.source_table_load_type	
		  ,m.[queue]
		  --,m.run_status
	
	from [dv_scheduler].[dv_run] r
	inner join [dv_scheduler].[dv_run_manifest] m
	on m.run_key = r.run_key
	left join [dv_scheduler].[dv_run_manifest_hierarchy] h
	on m.run_manifest_key = h.run_manifest_key
	left join [dv_scheduler].[dv_run_manifest] m1
	on m1.run_manifest_key = h.[run_manifest_prior_key]
	where 1=1
	and @run_key = @run_key
	and r.run_status = 'Scheduled'
	and m.run_status = 'Scheduled'
	and isnull(m1.run_status, 'Completed') <> 'Completed'
)