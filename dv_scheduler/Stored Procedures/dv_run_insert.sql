CREATE PROCEDURE [dv_scheduler].[dv_run_insert]
(
	@schedule_name		varchar(max) 
)
AS
BEGIN
SET NOCOUNT ON

DECLARE		@RC				varchar(max);
DECLARE		@run_key		int;

-- insert one row for the run into dv_run table
INSERT INTO [dv_scheduler].[dv_run] ([run_schedule_name]) VALUES (@schedule_name);

select @run_key = run_key from dv_scheduler.dv_run where run_key = SCOPE_IDENTITY();

-- execute dv_populate_run_manifest to insert data in dv_run_manifest table
EXECUTE @RC = [dv_scheduler].[dv_populate_run_manifest] 
   @schedule_name,
   @run_key

-- execute dv_populate_run_manifest_hierarchy to insert data in dv_run_manifest_hierarchy table
EXECUTE @RC = [dv_scheduler].[dv_populate_run_manifest_hierarchy] 
   @run_key

END