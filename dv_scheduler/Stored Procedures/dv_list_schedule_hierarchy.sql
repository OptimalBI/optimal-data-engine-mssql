
CREATE procedure [dv_scheduler].[dv_list_schedule_hierarchy]
( 
  @schedule_list varchar(4000)
)
as

BEGIN
set nocount on

declare @RN				int
       ,@_Message       nvarchar(512)
SELECT @RN = count(*) FROM [dv_scheduler].[fn_check_schedule_for_circular_reference](@schedule_list)
if @RN > 0
   begin
   select * FROM [dv_scheduler].[fn_check_schedule_for_circular_reference](@schedule_list)
   select @_Message = 'The Schedule: ' + @schedule_list + ' has Circular Reference and cannot be displayed'
   raiserror(@_Message, 16, 1)
   return
   end

;with wSchedule_Table as (
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
,wBaseSetPrior as (
select 
    schtp.[source_table_key] as table_key_prior
   ,scht.[source_table_key]
   ,source_table_name = cast(
						quotename(schtp.[source_system_name]) + '.' +  
						quotename(schtp.[source_table_schema]) + '.' +  
						quotename(schtp.[source_table_name]) as nvarchar(512))   
    
from wSchedule_Table schtp
left join [dv_scheduler].[vw_dv_source_table_hierarchy_current] sth
    on sth.[prior_table_key] = schtp.[source_table_key] 
left join wSchedule_Table scht
	on sth.[source_table_key] = scht.[source_table_key]
left join [dv_scheduler].[vw_dv_source_table_hierarchy_current] sthp
	on sthp.[source_table_key] = schtp.[source_table_key]
left join wSchedule_Table sthpst
	on sthpst.[source_table_key] = sthp.[prior_table_key] 
where sthpst.[source_table_key] is null
)
,wBaseSet as (
select  
    schtp.[source_table_key] as table_key_prior
   ,scht.[source_table_key]
   ,source_table_name = cast(
						quotename(schtp.[source_system_name]) + '.' +  
						quotename(schtp.[source_table_schema]) + '.' +  
						quotename(schtp.[source_table_name]) as nvarchar(512))   

from wSchedule_Table schtp
left join [dv_scheduler].[vw_dv_source_table_hierarchy_current] sth
    on sth.[prior_table_key] = schtp.[source_table_key] 
left join wSchedule_Table scht
	on sth.[source_table_key] = scht.[source_table_key]
)

,wBOM as (
select *
from wBaseSetPrior

union all
select
    b.table_key_prior
   ,b.[source_table_key]
   ,CAST(cte.source_table_name + ' >>> ' + b.source_table_name as nvarchar(512)) 
   
from wBaseSet b
inner join wBOM AS cte 
    ON cte.source_table_key = b.table_key_prior 
)

select source_table_name 
from wBOM
order by source_table_name
OPTION (MAXRECURSION 5000)
END