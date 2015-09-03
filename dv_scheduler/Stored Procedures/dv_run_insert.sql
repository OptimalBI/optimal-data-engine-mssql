USE [ODV_Config_Scheduler]
GO
/****** Object:  StoredProcedure [dv_scheduler].[dv_populate_run_manifest]    Script Date: 4/09/2015 9:52:50 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dv_scheduler].[dv_run_insert]
(
	@schedule_name		varchar(max) 
)
AS
BEGIN
SET NOCOUNT ON

DECLARE		@RC				varchar(max);
DECLARE		@run_key		int;

-- insert one row for the run into dv_run table
insert into dv_scheduler.dv_run default values;

select @run_key = run_key from dv_scheduler.dv_run where run_key = SCOPE_IDENTITY();

-- execute dv_populate_run_manifest to insert data in dv_run_manifest table
EXECUTE @RC = [dv_scheduler].[dv_populate_run_manifest] 
   @schedule_name,
   @run_key

-- execute dv_populate_run_manifest_hierarchy to insert data in dv_run_manifest_hierarchy table
EXECUTE @RC = [dv_scheduler].[dv_populate_run_manifest_hierarchy] 
   @run_key

END