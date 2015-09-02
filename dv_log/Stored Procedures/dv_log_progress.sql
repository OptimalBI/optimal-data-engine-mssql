
CREATE PROCEDURE [dv_log].[dv_log_progress]
(
  @vault_object_type             varchar(50)    = NULL
, @vault_object_name             varchar(128)   = NULL
, @vault_object_schema           varchar(128)   = NULL
, @vault_object_database         varchar(128)   = NULL
, @vault_source_name             varchar(128)   = NULL
, @vault_source_schema           varchar(128)   = NULL
, @vault_source_system           varchar(128)   = NULL
, @vault_execution_id            int            = NULL
, @vault_load_high_water		 datetimeoffset(7)  = NULL
--, @vault_load_finish_datetime	 datetimeoffset(7)  = NULL
--, @vault_load_duration			 int			= NULL
, @vault_rows_inserted			 int			= NULL
, @vault_rows_updated			 int			= NULL
, @vault_rows_deleted			 int			= NULL
)
AS
BEGIN
SET NOCOUNT ON

DECLARE @source_table_key	int
	   ,@object_key			int

set @source_table_key = 0

if @vault_object_type = 'sat'
begin
	select @source_table_key = st.table_key
	from [dbo].[dv_source_system] ss
	inner join [dbo].[dv_source_table] st
	on ss.system_key = st.system_key
	where 1=1
	and ss.source_system_name	= @vault_source_system
	and st.source_table_name	= @vault_source_name
	and st.source_table_schema	= @vault_source_schema
end

if @vault_object_type = 'sat'
begin
	select @object_key = satellite_key
	from [dbo].[dv_satellite]
	where 1=1
	and [satellite_database]	= @vault_object_database
	and [satellite_schema]		= @vault_object_schema
	and [satellite_name]		= @vault_object_name
end
if @vault_object_type = 'pit'
begin
   select @object_key = pit_key
   from [dbo].[dv_pit]
   where 1=1
    and [pit_database]		    = @vault_object_database
	and [pit_schema]			= @vault_object_schema
	and [pit_name]				= @vault_object_name
end

INSERT INTO [dv_log].[dv_load_state_history]
SELECT [load_state_key],left([Action], 1),[source_table_key],[object_key],[object_type],[execution_key],[load_high_water]--,[load_finish_datetime],[load_duration]
          ,[rows_inserted],[rows_updated],[rows_deleted],[updated_by],[update_date_time]
FROM(
MERGE INTO [dv_log].[dv_load_state] AS tgt                              
USING (VALUES 
		(@source_table_key 
		,@object_key 
		,@vault_object_type 
		,@vault_execution_id 
		,@vault_load_high_water 
		--,@vault_load_finish_datetime 
		--,@vault_load_duration 
		,@vault_rows_inserted 
		,@vault_rows_updated 
		,@vault_rows_deleted)) 
AS src	([source_table_key]
		,[object_key]
		,[object_type]
		,[execution_key]
		,[load_high_water]
		--,[load_finish_datetime]
		--,[load_duration]
		,[rows_inserted]
		,[rows_updated]
		,[rows_deleted])
ON 
    src.[source_table_key]		= tgt.[source_table_key]
and src.[object_key]			= tgt.[object_key]
and src.[object_type]			= tgt.[object_type]
and src.[execution_key]			= tgt.[execution_key]
                             
WHEN MATCHED THEN UPDATE
SET 
	 tgt.[load_high_water]		= src.[load_high_water]
	--,tgt.[load_finish_datetime]	= src.[load_finish_datetime]
	--,tgt.[load_duration]		= src.[load_duration]
	,tgt.[rows_inserted]		= src.[rows_inserted]
	,tgt.[rows_updated]			= src.[rows_updated]
	,tgt.[rows_deleted]			= src.[rows_deleted]
	,tgt.[update_date_time]     = sysdatetimeoffset()
                              
WHEN NOT MATCHED THEN INSERT
	([source_table_key]
	,[object_key]
	,[object_type]
	,[execution_key]
	,[load_high_water]
	--,[load_finish_datetime]
	--,[load_duration]
	,[rows_inserted]
	,[rows_updated]
	,[rows_deleted])
VALUES (
	 src.[source_table_key]
	,src.[object_key]
	,src.[object_type]
	,src.[execution_key]
	,src.[load_high_water]
	--,src.[load_finish_datetime]
	--,src.[load_duration]
	,src.[rows_inserted]
	,src.[rows_updated]
	,src.[rows_deleted])
output $action, inserted.*
) 
as changes 
([Action], [load_state_key],[source_table_key],[object_key],[object_type],[execution_key],[load_high_water]--,[load_finish_datetime],[load_duration]
          ,[rows_inserted],[rows_updated],[rows_deleted],[updated_by],[update_date_time])
;
END