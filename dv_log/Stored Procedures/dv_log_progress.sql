

CREATE PROCEDURE [dv_log].[dv_log_progress]
(
  @vault_object_type             varchar(50)    = NULL
, @vault_object_name             varchar(128)   = NULL
, @vault_object_schema           varchar(128)   = NULL
, @vault_object_database         varchar(128)   = NULL
, @vault_source_unique_name      varchar(128)   = NULL
, @vault_execution_id            int            = NULL
, @vault_runkey					 int            = NULL
, @vault_load_high_water		 datetimeoffset(7)  = NULL
, @vault_lookup_start_datetime	 datetimeoffset(7)  = NULL
, @vault_load_start_datetime	 datetimeoffset(7)  = NULL
, @vault_load_finish_datetime	 datetimeoffset(7)  = NULL
, @vault_rows_inserted			 int			= NULL
, @vault_rows_updated			 int			= NULL
, @vault_rows_deleted			 int			= NULL
, @vault_rows_affected			 int			= NULL
)
AS
BEGIN
SET NOCOUNT ON

DECLARE @source_table_key	int
	   ,@object_key			int

set @source_table_key = 0

if @vault_object_type in('hub', 'sat', 'lnk', 'stg')
begin
	select @source_table_key = st.[source_table_key]
	from [dbo].[dv_source_table] st
	where 1=1
	and st.source_unique_name	= @vault_source_unique_name

	if @vault_object_type = 'sat'
	begin
		select @object_key = satellite_key
		from [dbo].[dv_satellite]
		where 1=1
		and [satellite_database]	= @vault_object_database
		and [satellite_schema]		= @vault_object_schema
		and [satellite_name]		= @vault_object_name
	end
	else if @vault_object_type = 'hub'
	begin
		select @object_key = hub_key
		from [dbo].[dv_hub]
		where 1=1
		and [hub_database]		= @vault_object_database
		and [hub_schema]		= @vault_object_schema
		and [hub_name]			= @vault_object_name
	end
	else if @vault_object_type = 'lnk'
	begin
		select @object_key = link_key
		from [dbo].[dv_link]
		where 1=1
		and [link_database]		= @vault_object_database
		and [link_schema]		= @vault_object_schema
		and [link_name]			= @vault_object_name
	end
	else if @vault_object_type = 'stg'
	begin
		select @object_key = source_table_key
		from [dbo].[vw_stage_table]
		where 1=1
		and [stage_database]		= @vault_object_database
		and [stage_schema]			= @vault_object_schema
		and [stage_table_name]		= @vault_object_name
	end
end
else
RAISERROR('Attempted to Log and Unsupported Object Type: %s', 16, 1, @vault_object_type)


INSERT INTO [dv_log].[dv_load_state_history]
SELECT [load_state_key],left([Action], 1),[source_table_key],[object_key],[object_type],[execution_key],[run_key], [load_high_water],[lookup_start_datetime],[load_start_datetime],[load_end_datetime],[rows_inserted],[rows_updated],[rows_deleted],[rows_affected],[updated_by],[update_date_time]
FROM(
MERGE INTO [dv_log].[dv_load_state] AS tgt                              
USING (VALUES 
		(@source_table_key 
		,@object_key 
		,@vault_object_type 
		,@vault_execution_id
		,@vault_runkey 
		,@vault_load_high_water
		,@vault_lookup_start_datetime
		,@vault_load_start_datetime
		,@vault_load_finish_datetime 
		,@vault_rows_inserted 
		,@vault_rows_updated 
		,@vault_rows_deleted
		,@vault_rows_affected)) 
AS src	([source_table_key]
		,[object_key]
		,[object_type]		
		,[execution_key]
		,[run_key]
		,[load_high_water]
		,[lookup_start_datetime]
		,[load_start_datetime]
		,[load_end_datetime]
		,[rows_inserted]
		,[rows_updated]
		,[rows_deleted]
		,[rows_affected])
ON 
    src.[source_table_key]		= tgt.[source_table_key]
and src.[object_key]			= tgt.[object_key]
and src.[object_type]			= tgt.[object_type]
                             
WHEN MATCHED THEN UPDATE
SET 
	 tgt.[load_high_water]		= src.[load_high_water]
	,tgt.[lookup_start_datetime]= src.[lookup_start_datetime]
	,tgt.[load_start_datetime]	= src.[load_start_datetime]
	,tgt.[load_end_datetime]	= src.[load_end_datetime]
	,tgt.[execution_key]		= src.[execution_key]
	,tgt.[run_key]				= src.[run_key]
	,tgt.[rows_inserted]		= src.[rows_inserted]
	,tgt.[rows_updated]			= src.[rows_updated]
	,tgt.[rows_deleted]			= src.[rows_deleted]
	,tgt.[rows_affected]		= src.[rows_affected]
	,tgt.[update_date_time]     = sysdatetimeoffset()
                              
WHEN NOT MATCHED THEN INSERT
	([source_table_key]
	,[object_key]
	,[object_type]
	,[execution_key]
	,[run_key]
	,[load_high_water]
	,[lookup_start_datetime]
	,[load_start_datetime]
	,[load_end_datetime]
	,[rows_inserted]
	,[rows_updated]
	,[rows_deleted]
	,[rows_affected])
VALUES (
	 src.[source_table_key]
	,src.[object_key]
	,src.[object_type]
	,src.[execution_key]
	,src.[run_key]
	,src.[load_high_water]
	,src.[lookup_start_datetime]
	,src.[load_start_datetime]
	,src.[load_end_datetime]
	,src.[rows_inserted]
	,src.[rows_updated]
	,src.[rows_deleted]
	,src.[rows_affected])
output $action, inserted.*
) 
as changes 
([Action], [load_state_key],[source_table_key],[object_key],[object_type],[execution_key],[run_key],[load_high_water],[lookup_start_datetime],[load_start_datetime],[load_end_datetime],[rows_inserted],[rows_updated],[rows_deleted],[rows_affected],[updated_by],[update_date_time])
;
END