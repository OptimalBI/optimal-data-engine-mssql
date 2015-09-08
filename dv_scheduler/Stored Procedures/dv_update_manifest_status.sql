CREATE PROCEDURE [dv_scheduler].[dv_update_manifest_status]
(
  @vault_run_key				 int			= NULL
, @vault_source_system_name      varchar(50)	= NULL
, @vault_source_table_schema     varchar(128)   = NULL
, @vault_source_table_name       varchar(128)   = NULL
, @vault_run_status				 varchar(20)	= NULL
, @dogenerateerror               bit            = 0
, @dothrowerror                  bit			= 1
)
AS
BEGIN
SET NOCOUNT ON

if @vault_run_status in ('Queued', 'Failed')
	UPDATE [dv_scheduler].[dv_run_manifest]
			SET   [run_status] = @vault_run_status
			WHERE [run_key] = @vault_run_key
			  AND [source_system_name]	= @vault_source_system_name
			  AND [source_table_schema] = @vault_source_table_schema
              AND [source_table_name]	= @vault_source_table_name
else if @vault_run_status in ('Processing')
	UPDATE [dv_scheduler].[dv_run_manifest]
			SET [run_status] = @vault_run_status
			   ,[start_datetime] = SYSDATETIMEOFFSET()
               ,[session_id] = @@SPID
			WHERE [run_key] = @vault_run_key
			  AND [source_system_name] = @vault_source_system_name
			  AND [source_table_schema] = @vault_source_table_schema
              AND [source_table_name] = @vault_source_table_name
else if @vault_run_status in ('Completed')
    UPDATE [dv_scheduler].[dv_run_manifest]
			SET [completed_datetime] = SYSDATETIMEOFFSET()
               ,[run_status] = 'Completed'
			WHERE [run_key] = @vault_run_key
			  AND [source_system_name] = @vault_source_system_name
			  AND [source_table_schema] = @vault_source_table_schema
              AND [source_table_name] = @vault_source_table_name
else
    raiserror('@vault_run_status must be one of: (Processing, Queued, Failed, Completed)', 16, 1)



END