CREATE FUNCTION [dv_scripting].[fn_get_task_log_insert_statement] 
(@source_version_key	INT
,@object_type			VARCHAR(128)
,@object_key			INT
,@declare_variables     BIT)
/********************************************************************************************
This takes a source_version_key, and object_type (hub, link, sat, stage) and an object key
and outputs a logging script, for logging progerss, outcomes and high water marks to the local database.
For the generated script ot work, the following runtime variables need to be present and populated in the calling script:
	@source_date_time
   ,@load_start_date
   ,@load_end_date
   ,@rows_inserted
   ,@rows_updated
   ,@rows_deleted
   ,@vault_runkey

@__source_high_water_lsn has been modified to be binary(10) (instead of varchar(50)) in order to support an external fix.
********************************************************************************************/
RETURNS varchar(2048)
AS
BEGIN
DECLARE @SQL					VARCHAR(MAX)	= ''
       ,@crlf					CHAR(2)			= CHAR(13) + CHAR(10)
	   ,@vault_database			VARCHAR(128)
	   ,@object_name			VARCHAR(128)
	   ,@source_unique_name		VARCHAR(128)
	   ,@source_table_key       INT
	   ,@procedure_name			VARCHAR(128)

SELECT @source_unique_name = st.source_unique_name
      ,@source_table_key = st.source_table_key
FROM [dbo].[dv_source_table] st
INNER JOIN [dbo].[dv_source_version] sv ON sv.source_table_key = st.source_table_key                         
                                       AND sv.is_current = 1
WHERE sv.source_version_key = @source_version_key
IF @object_type IN('hub', 'hublookup')
   SELECT @object_name = [hub_name]
         ,@vault_database = [hub_database]
   FROM [dbo].[dv_hub]
   WHERE [hub_key] = @object_key
ELSE 
IF @object_type IN('link', 'linklookup') 
   SELECT @object_name = [link_name]
         ,@vault_database = [link_database]
   FROM [dbo].[dv_link]
   WHERE [link_key] = @object_key
ELSE 
IF @object_type = 'sat' 
   SELECT @object_name = [satellite_name]
         ,@vault_database = [satellite_database]
   FROM [dbo].[dv_satellite]
   WHERE [satellite_key] = @object_key
ELSE 
IF @object_type = 'stage' 
   SELECT  @object_name = ss.stage_schema_name + '.' + st.stage_table_name
          ,@vault_database = sd.stage_database_name
		  ,@procedure_name = sv.source_procedure_name
	FROM [dbo].[dv_source_version] sv
	INNER JOIN [dbo].[dv_source_table] st ON sv.source_table_key = st.source_table_key                         
                                       AND sv.is_current = 1
    INNER JOIN [dbo].[dv_stage_schema] ss ON ss.stage_schema_key = st.stage_schema_key
	INNER JOIN [dbo].[dv_stage_database] sd ON sd.stage_database_key = ss.stage_database_key
	WHERE sv.source_version_key = @source_version_key
 
SELECT @procedure_name = CASE WHEN ISNULL(@procedure_name, '') = '' THEN '<N/A>' ELSE @procedure_name END
IF @declare_variables = 1
BEGIN
	SET @SQL += 'DECLARE ' + @crlf
	SET @SQL += '  @__high_water_date        datetimeoffset(7)' + @crlf
	SET @SQL += ', @__source_high_water_date varchar(50)' + @crlf
	SET @SQL += ', @__source_high_water_lsn  binary(10)' + @crlf
	SET @SQL += ', @__load_start_date        varchar(50)' + @crlf
	SET @SQL += ', @__load_end_date          varchar(50)' + @crlf
	SET @SQL += ', @__rows_inserted          bigint' + @crlf
	SET @SQL += ', @__rows_updated           bigint' + @crlf
	SET @SQL += ', @__rows_deleted           bigint' + @crlf
	SET @SQL += ', @__vault_runkey           bigint' + @crlf
END
ELSE
BEGIN
	SET @SQL += 'INSERT ' + QUOTENAME(@vault_database) + '.[dbo].[dv_task_state] ([source_table_key],[source_unique_name],[object_key],[object_type],[object_name],[procedure_name],[high_water_date],[source_high_water_lsn],[source_high_water_date],[task_start_datetime],[task_end_datetime],[rows_inserted],[rows_updated],[rows_deleted],[session_id],[run_key])' + @crlf
	SET @SQL += 'VALUES(' + cast(@source_table_key as varchar(20)) + ',' + @crlf
	SET @SQL += '''' + @source_unique_name + ''',' + @crlf
	SET @SQL += cast(@object_key as varchar(20)) + ',' + @crlf
	SET @SQL += ''''+ @object_type + ''',' + @crlf
	SET @SQL += '''' + @object_name + ''',' + @crlf
	SET @SQL += '''' + @procedure_name + ''',' + @crlf
	SET @SQL += '@__high_water_date,' + @crlf
	SET @SQL += 'CONVERT(binary(10), @__source_high_water_lsn,1),' + @crlf
	SET @SQL += 'CONVERT(varchar(50), @__source_high_water_date),' + @crlf
	SET @SQL += '@__load_start_date,' + @crlf
	SET @SQL += '@__load_end_date,' + @crlf
	SET @SQL += '@__rows_inserted,' + @crlf
	SET @SQL += '@__rows_updated,' + @crlf
	SET @SQL += '@__rows_deleted,' + @crlf
	SET @SQL += 'CAST(@@SPID AS VARCHAR(20)),' + @crlf
	SET @SQL += '@__vault_runkey)' + @crlf
END
RETURN @SQL

END