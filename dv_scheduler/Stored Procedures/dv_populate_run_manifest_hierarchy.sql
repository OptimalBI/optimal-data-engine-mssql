USE [ODV_Config_Scheduler]
GO
/****** Object:  StoredProcedure [dv_scheduler].[dv_populate_run_manifest]    Script Date: 4/09/2015 9:52:50 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dv_scheduler].[dv_populate_run_manifest_hierarchy]
(
	@run_key			int
)
AS
BEGIN
SET NOCOUNT ON

;with cte_manifest_current as
(select src_table_hierarchy.source_table_hierarchy_key,run_mani.run_manifest_key 
from dv_scheduler.dv_source_table_hierarchy src_table_hierarchy
inner join dv_scheduler.dv_run_manifest run_mani
on src_table_hierarchy.source_table_key = run_mani.source_table_key
where run_mani.run_key = @run_key
),
cte_manifest_prior as (
select src_table_hierarchy_prior.source_table_hierarchy_key, run_mani_prior.run_manifest_key as prior_manifest_key
from dv_scheduler.dv_source_table_hierarchy src_table_hierarchy_prior
inner join dv_scheduler.dv_run_manifest run_mani_prior
on src_table_hierarchy_prior.prior_table_key = run_mani_prior.source_table_key
where run_mani_prior.run_key = @run_key
) 
insert into dv_scheduler.dv_run_manifest_hierarchy (run_manifest_key, run_manifest_prior_key)
select mani_cur.run_manifest_key, mani_prev.prior_manifest_key  
from cte_manifest_current mani_cur
inner join cte_manifest_prior mani_prev
on mani_cur.source_table_hierarchy_key = mani_prev.source_table_hierarchy_key;

SET NOCOUNT OFF

END;