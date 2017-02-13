CREATE PROCEDURE [dv_scheduler].[dv_manifest_status_update]
(
  @vault_run_key				 int			= NULL
, @vault_source_unique_name      varchar(128)	= NULL
, @vault_run_status				 varchar(20)	= NULL
, @dogenerateerror               bit            = 0
, @dothrowerror                  bit			= 1
)
AS
BEGIN
SET NOCOUNT ON

if @vault_run_status in ('Queued', 'Failed', 'Cancelled')
	UPDATE [dv_scheduler].[dv_run_manifest]
			SET   [run_status] = @vault_run_status
			WHERE [run_key] = @vault_run_key
			  AND [source_unique_name]	= @vault_source_unique_name
else if @vault_run_status in ('Processing')
	UPDATE [dv_scheduler].[dv_run_manifest]
			SET [run_status] = @vault_run_status
			   ,[start_datetime] = SYSDATETIMEOFFSET()
               ,[session_id] = @@SPID
			WHERE [run_key] = @vault_run_key
			  AND [source_unique_name]	= @vault_source_unique_name
else if @vault_run_status in ('Completed')
    UPDATE [dv_scheduler].[dv_run_manifest]
			SET [completed_datetime] = SYSDATETIMEOFFSET()
               ,[run_status] = 'Completed'
			WHERE [run_key] = @vault_run_key
			  AND [source_unique_name]	= @vault_source_unique_name
else
    raiserror('@vault_run_status must be one of: (Processing, Queued, Failed, Completed, Cancelled)', 16, 1)



END