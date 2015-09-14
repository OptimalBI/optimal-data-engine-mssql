
CREATE procedure [dv_scheduler].[dv_list_manifest_hierarchy]
( 
  @vault_run_key int
)
as

BEGIN
set nocount on

declare @RN				int
       ,@_Message       nvarchar(512)
SELECT @RN = count(*) FROM [dv_scheduler].[fn_check_manifest_for_circular_reference] (@vault_run_key)
if @RN > 0
   begin
   select * from [dv_scheduler].[fn_check_manifest_for_circular_reference] (@vault_run_key)
   select @_Message = 'The Manifest for run_key ' + cast(@vault_run_key as varchar(20)) + ' has Circular Reference and cannot be displayed'
   raiserror(@_Message, 16, 1)
   return
   end

;with  wBaseSetPrior as (
select
    mp.run_manifest_key as run_manifest_key_prior
   ,m.run_manifest_key
   ,source_table_name = cast(
							quotename(mp.[source_system_name]) + '.' +  
							quotename(mp.[source_table_schema]) + '.' +  
							quotename(mp.[source_table_name])  
						as nvarchar(512))   
 
from [dv_scheduler].[dv_run] r
inner join [dv_scheduler].[dv_run_manifest] mp
	on r.run_key = mp.run_key
left join [dv_scheduler].[dv_run_manifest_hierarchy] mh
    on mh.run_manifest_prior_key = mp.run_manifest_key 
left join [dv_scheduler].[dv_run_manifest] m
    on m.run_manifest_key = mh.run_manifest_key
left join [dv_scheduler].[dv_run_manifest_hierarchy] mhp
	on mhp.run_manifest_key = mp.run_manifest_key

where 1=1
  and r.run_key = @vault_run_key
  and mhp.run_manifest_prior_key is null
)
,wBaseSet as (
select
    mp.run_manifest_key as run_manifest_key_prior
   ,m.run_manifest_key
   ,source_table_name = cast(
							quotename(mp.[source_system_name]) + '.' +  
							quotename(mp.[source_table_schema]) + '.' +  
							quotename(mp.[source_table_name])  
						as nvarchar(512))   
    
from [dv_scheduler].[dv_run] r
inner join [dv_scheduler].[dv_run_manifest] mp
	on r.run_key = mp.run_key
left join [dv_scheduler].[dv_run_manifest_hierarchy] mh
    on mh.run_manifest_prior_key = mp.run_manifest_key 
left join [dv_scheduler].[dv_run_manifest] m
    on m.run_manifest_key = mh.run_manifest_key
where r.run_key = @vault_run_key
)
,wBOM as (
select *
from wBaseSetPrior

union all
select
    b.run_manifest_key_prior
   ,b.run_manifest_key
   ,CAST(cte.source_table_name + ' >>> ' + b.source_table_name as nvarchar(512)) 
   
from wBaseSet b
inner join wBOM AS cte 
    ON cte.run_manifest_key = b.run_manifest_key_prior
)

select source_table_name 
from wBOM
order by source_table_name
OPTION (MAXRECURSION 5000)
END