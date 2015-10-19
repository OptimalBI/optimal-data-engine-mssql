Create FUNCTION [dv_scheduler].[fn_check_schedule_for_circular_reference](@schedule_list varchar(4000))

RETURNS TABLE	
AS
RETURN
with wSchedule_Table as (
      select s.schedule_key
	        ,st.[source_table_key]
	        ,ss.source_system_name
	        ,st.source_table_schema
			,st.source_table_name
	  from [dv_scheduler].[vw_dv_schedule_current] s
	  inner join [dv_scheduler].[vw_dv_schedule_source_table_current] sst
	  on sst.schedule_key = s.schedule_key
	  inner join [dbo].[dv_source_table] st
	  on st.[source_table_key] = sst.source_table_key
	  inner join [dbo].[dv_source_system] ss
	  on ss.[source_system_key] = st.system_key
	  where s.schedule_name in(select ltrim(rtrim(Item)) FROM [dbo].[fn_split_strings] (@schedule_list, ','))
	)
,wBaseSet as (
select sth.[source_table_key]
      ,sth.[prior_table_key]

from wSchedule_Table schtp
left join [dv_scheduler].[vw_dv_source_table_hierarchy_current] sth
    on sth.[source_table_key] = schtp.[source_table_key] 
where 1=1
  and coalesce(sth.[source_table_key],sth.[prior_table_key]) is not null
) 
,wFindRoot AS
(
    SELECT [source_table_key],[prior_table_key], CAST([source_table_key] as nvarchar(max)) BreadCrumb, 0 Distance
    FROM wBaseSet

    UNION ALL

    SELECT c.[source_table_key], p.[prior_table_key], c.BreadCrumb + N' > ' + cast(p.[prior_table_key] as nvarchar(max)), c.Distance + 1
    FROM wBaseSet p
    JOIN wFindRoot c
    ON c.[prior_table_key] = p.[source_table_key] AND p.[prior_table_key] <> p.[source_table_key] AND c.[prior_table_key] <> c.[source_table_key]
 )
select *
FROM wFindRoot r
WHERE 1=1
  and r.[source_table_key] = r.[prior_table_key] 
  AND r.[prior_table_key] <> 0
  AND r.Distance > 0