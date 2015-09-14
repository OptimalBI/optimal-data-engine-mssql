
CREATE FUNCTION [dv_scheduler].[fn_get_waiting_scheduler_tasks]
(@run_key int
,@runnable varchar(10) = 'Runnable' 
)
RETURNS TABLE 
AS
RETURN 
(
-- 'Potential' tells the Function to look for tasks, for which all precedents are either Completed, Queued or Processing (and can Potentially be added to the queue for running).
-- Any other Value looks for tasks, for which all precendents are Completed (and can therefore be placed on the queue for processing immediately)
select m.source_system_name
      ,m.source_timevault
	  ,m.source_table_schema	
	  ,m.source_table_name	
	  ,m.source_procedure_schema
	  ,m.source_procedure_name
	  ,m.source_table_load_type	
	  ,m.[queue]
	from [dv_scheduler].[dv_run] r
	inner join [dv_scheduler].[dv_run_manifest] m
	on m.run_key = r.run_key
	left join [dv_scheduler].[dv_run_manifest_hierarchy] h
	on m.run_manifest_key = h.run_manifest_key
	left join [dv_scheduler].[dv_run_manifest] m1
	on m1.run_manifest_key = h.[run_manifest_prior_key]
	where 1=1
	and r.run_key = @run_key
	and r.run_status = 'Started'
	and m.run_status = 'Scheduled'
	and 'Completed' = 
		case when @runnable <> 'Potential' 
	         then 
			      case when m1.run_status is null then 'Completed'
					   when m1.run_status = 'Completed' then 'Completed'
					   else 'Unknown'
					   end
			 else	
				  case when m1.run_status is null then 'Completed'
				       when m1.run_status = 'Completed' then 'Completed'
					   when m1.run_status = 'Queued'    then 'Completed'
					   when m1.run_status = 'Processing' then 'Completed'
					   else 'Unknown'
					   end
			end
			 
except
select m.source_system_name
	  ,m.source_timevault	
	  ,m.source_table_schema	
	  ,m.source_table_name	
	  ,m.source_procedure_schema
	  ,m.source_procedure_name
	  ,m.source_table_load_type	
	  ,m.[queue]
	from [dv_scheduler].[dv_run] r
	inner join [dv_scheduler].[dv_run_manifest] m
	on m.run_key = r.run_key
	left join [dv_scheduler].[dv_run_manifest_hierarchy] h
	on m.run_manifest_key = h.run_manifest_key
	left join [dv_scheduler].[dv_run_manifest] m1
	on m1.run_manifest_key = h.[run_manifest_prior_key]
	where 1=1
	and r.run_key = @run_key
	and r.run_status = 'Started'
	and m.run_status = 'Scheduled'
	and not 'Completed'  =
		case when @runnable <> 'Potential' 
	         then 
			      case when m1.run_status is null then 'Completed'
					   when m1.run_status = 'Completed' then 'Completed'
					   else 'Unknown'
					   end
			 else	
				  case when m1.run_status is null then 'Completed'
				       when m1.run_status = 'Completed' then 'Completed'
					   when m1.run_status = 'Queued'    then 'Completed'
					   when m1.run_status = 'Processing' then 'Completed'
					   else 'Unknown'
					   end
				  end

)