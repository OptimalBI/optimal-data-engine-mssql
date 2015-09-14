CREATE procedure [dv_scheduler].[dv_list_schedule_dependencies]
(
@schedule_name varchar(max)
)
as

BEGIN
set nocount on


if @schedule_name is null
begin 
    select @schedule_name = stuff((select ',' + s.schedule_name from dv_scheduler.dv_schedule s for xml path('')),1,1,'')
end
;with wBaseSetPrior as (
select
 mp.table_key as prior_table_key
,m.table_key
,source_table_name = cast(
quotename(ss.[source_system_name]) + '.' +
quotename(mp.[source_table_schema]) + '.' +
quotename(mp.[source_table_name])
as nvarchar(512))
from [dv_scheduler].[vw_dv_schedule_current] s
inner join [dv_scheduler].[vw_dv_schedule_source_table_current] ssts
on ssts.schedule_key = s.schedule_key
inner join [dbo].[dv_source_table] mp
on mp.table_key = ssts.source_table_key
inner join [dbo].[dv_source_system] ss
on ss.system_key = mp.system_key
left join [dv_scheduler].[vw_dv_source_table_hierarchy_current] mh
on mh.prior_table_key = mp.table_key
left join [dbo].[dv_source_table] m
on m.table_key = mh.source_table_key
left join [dv_scheduler].[vw_dv_source_table_hierarchy_current] mhp
on mhp.source_table_key = mp.table_key
where 1=1
and s.schedule_name in (select replace(Item,' ','') from dbo.fn_split_strings(@schedule_name,','))
and mhp.prior_table_key is null
--and (s.is_deleted | ssts.is_deleted | isnull(mh.is_deleted, 0) | isnull(mhp.is_deleted, 0) = 0)
)
,wBaseSet as (
select
 mp.table_key as prior_table_key
,m.table_key
,source_table_name = cast(
quotename(ss.[source_system_name]) + '.' +
quotename(mp.[source_table_schema]) + '.' +
quotename(mp.[source_table_name])
as nvarchar(512))
from [dv_scheduler].[vw_dv_schedule_current] s
inner join [dv_scheduler].[vw_dv_schedule_source_table_current] ssts
on ssts.schedule_key = s.schedule_key
inner join [dbo].[dv_source_table] mp
on mp.table_key = ssts.source_table_key
inner join [dbo].[dv_source_system] ss
on ss.system_key = mp.system_key
left join [dv_scheduler].[vw_dv_source_table_hierarchy_current] mh
on mh.prior_table_key = mp.table_key
left join [dbo].[dv_source_table] m
on m.table_key = mh.source_table_key
where 1=1
and s.schedule_name in (select replace(Item,' ','') from dbo.fn_split_strings(@schedule_name,','))
--and (s.is_deleted | ssts.is_deleted | isnull(mh.is_deleted, 0)  = 0)
)
,wBOM as (
select *
from wBaseSetPrior

union all
select
 b.prior_table_key
,b.table_key
,CAST(cte.source_table_name + ' >>> ' + b.source_table_name as nvarchar(512))
from wBaseSet b
inner join wBOM AS cte
ON cte.table_key = b.prior_table_key
)
select distinct source_table_name
from wBOM
order by source_table_name
OPTION (MAXRECURSION 5000);

END;