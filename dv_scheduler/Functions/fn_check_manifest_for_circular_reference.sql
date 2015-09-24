
CREATE FUNCTION [dv_scheduler].[fn_check_manifest_for_circular_reference](@run_key int)

RETURNS TABLE	
AS
RETURN

with wBaseSet as (
select mh.run_manifest_key
      ,mh.run_manifest_prior_key

from [dv_scheduler].[dv_run] r
inner join [dv_scheduler].[dv_run_manifest] m
on r.run_key = m.run_key
left join [dv_scheduler].[dv_run_manifest_hierarchy] mh
on m.run_manifest_key = mh.run_manifest_key
where 1=1
  and m.run_key = @run_key
  and coalesce(mh.run_manifest_key,mh.run_manifest_prior_key) is not null
) 
,wFindRoot AS
(
    SELECT run_manifest_key,run_manifest_prior_key, CAST(run_manifest_prior_key as nvarchar(max)) BreadCrumb, 0 Distance
    FROM wBaseSet

    UNION ALL

    SELECT c.run_manifest_key, p.run_manifest_prior_key, c.BreadCrumb + N' > ' + cast(p.run_manifest_prior_key as nvarchar(max)), c.Distance + 1
    FROM wBaseSet p
    JOIN wFindRoot c
    ON c.run_manifest_prior_key = p.run_manifest_key AND p.run_manifest_prior_key <> p.run_manifest_key AND c.run_manifest_prior_key <> c.run_manifest_key
 )
select *
FROM wFindRoot r
WHERE r.run_manifest_key = r.run_manifest_prior_key 
  AND r.run_manifest_prior_key <> 0
  AND r.Distance > 0