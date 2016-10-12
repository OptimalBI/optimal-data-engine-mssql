CREATE PROCEDURE [dbo].[dv_load_sat_table]
(
  @vault_source_system_name		varchar(128) = NULL
, @vault_source_table_schema	varchar(128) = NULL
, @vault_source_table_name		varchar(128) = NULL
, @vault_sat_name				varchar(128) = NULL
, @vault_temp_table_name        varchar(116) = NULL 
, @vault_source_load_type		varchar(50)  = NULL 
, @vault_sql_statement          nvarchar(max) OUTPUT
, @dogenerateerror				bit				= 0
, @dothrowerror					bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly
-- System Wide Defaults
-- Local Defaults Values
DECLARE @crlf								char(2)			= CHAR(13) + CHAR(10)
-- Global Defaults
DECLARE  
		 @def_global_lowdate				datetime
        ,@def_global_highdate				datetime
        ,@def_global_default_load_date_time	varchar(128)
		,@def_global_failed_lookup_key		int
-- Hub Defaults									
        ,@def_hub_prefix					varchar(128)
		,@def_hub_schema					varchar(128)
		,@def_hub_filegroup					varchar(128)
--Link Defaults									
		,@def_link_prefix					varchar(128)
		,@def_link_schema					varchar(128)
		,@def_link_filegroup				varchar(128)
--Sat Defaults									
		,@def_sat_prefix					varchar(128)
		,@def_sat_schema					varchar(128)
		,@def_sat_filegroup					varchar(128)
		,@sat_start_date_col				varchar(128)
		,@sat_end_date_col					varchar(128)
		,@sat_current_row_col				varchar(128)				

-- Object Specific Settings
-- Source Table
		,@source_system						varchar(128)
		,@source_database					varchar(128)
		,@source_schema						varchar(128)
		,@source_table						varchar(128)
		,@source_table_config_key			int
		,@source_qualified_name				varchar(512)
		,@source_load_date_time				varchar(128)
		,@source_load_type					varchar(50)
		,@source_payload					nvarchar(max)
-- Hub Table
		,@hub_database						varchar(128)
		,@hub_schema						varchar(128)
		,@hub_table							varchar(128)
		,@hub_surrogate_keyname				varchar(128)
		,@hub_config_key					int
		,@hub_qualified_name				varchar(512)
		,@hubt_technical_columns			nvarchar(max)
-- Link Table
		,@link_database						varchar(128)
		,@link_schema						varchar(128)
		,@link_table						varchar(128)
		,@link_surrogate_keyname			varchar(128)
		,@link_config_key					int
		,@link_qualified_name				varchar(512)
		,@link_technical_columns			nvarchar(max)
		,@link_lookup_joins					nvarchar(max)
		,@link_hub_keys						nvarchar(max)
-- Sat Table
		,@sat_database						varchar(128)
		,@sat_schema						varchar(128)
		,@sat_table							varchar(128)
		,@sat_surrogate_keyname				varchar(128)
		,@sat_config_key					int
		,@sat_link_hub_flag					char(1)
		,@sat_qualified_name				varchar(512)
		,@sat_source_date_time				varchar(50)
		,@sat_technical_columns				nvarchar(max)
		,@sat_payload						nvarchar(max)

--  Working Storage
DECLARE @execution_id				int
       ,@rows_inserted				int
	   ,@rows_updated				int
	   ,@rows_deleted				int
	   ,@load_duration				int
	   ,@load_start_time            datetimeoffset(7)
	   ,@load_finish_time			datetimeoffset(7)
	   ,@load_high_water			datetimeoffset(7)
DECLARE @sat_insert_count			int
	   ,@sql						nvarchar(max)
	   ,@sql1						nvarchar(max)
	   ,@sql2						nvarchar(max)
	   ,@surrogate_key_match        nvarchar(max)
DECLARE @declare					nvarchar(512)	= ''
DECLARE @count_rows					nvarchar(256)	= ''
DECLARE @match_list					nvarchar(max)	= ''
DECLARE @value_list					nvarchar(max)	= ''
DECLARE @sat_column_list			nvarchar(max)	= ''
DECLARE @hub_column_list			nvarchar(max)	= ''

DECLARE @ParmDefinition				nvarchar(500);

DECLARE @wrk_link_joins			varchar(4000)
DECLARE @wrk_link__keys			varchar(4000)
-- Log4TSQL Journal Constants 										
DECLARE @SEVERITY_CRITICAL      smallint = 1;
DECLARE @SEVERITY_SEVERE        smallint = 2;
DECLARE @SEVERITY_MAJOR         smallint = 4;
DECLARE @SEVERITY_MODERATE      smallint = 8;
DECLARE @SEVERITY_MINOR         smallint = 16;
DECLARE @SEVERITY_CONCURRENCY   smallint = 32;
DECLARE @SEVERITY_INFORMATION   smallint = 256;
DECLARE @SEVERITY_SUCCESS       smallint = 512;
DECLARE @SEVERITY_DEBUG         smallint = 1024;
DECLARE @NEW_LINE               char(1)  = CHAR(10);

-- Log4TSQL Standard/ExceptionHandler variables
DECLARE	  @_Error         int
		, @_RowCount      int
		, @_Step          varchar(128)
		, @_Message       nvarchar(512)
		, @_ErrorContext  nvarchar(512)

-- Log4TSQL JournalWriter variables
DECLARE   @_FunctionName			varchar(255)
		, @_SprocStartTime			datetime
		, @_JournalOnOff			varchar(3)
		, @_Severity				smallint
		, @_ExceptionId				int
		, @_StepStartTime			datetime
		, @_ProgressText			nvarchar(max)

SET @_Error             = 0;
SET @_FunctionName      = OBJECT_NAME(@@PROCID);
SET @_Severity          = @SEVERITY_INFORMATION;
SET @_SprocStartTime    = sysdatetimeoffset();
SET @_ProgressText      = '' 
SET @_JournalOnOff      = log4.GetJournalControl(@_FunctionName, 'HOWTO');  -- left Group Name as HOWTO for now.


-- set Log4TSQL Parameters for Logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @vault_source_system_name	 : ' + COALESCE(@vault_source_system_name, 'NULL')
						+ @NEW_LINE + '    @vault_source_table_schema    : ' + COALESCE(@vault_source_table_schema, 'NULL')
						+ @NEW_LINE + '    @vault_source_table_name      : ' + COALESCE(@vault_source_table_name, 'NULL')
						+ @NEW_LINE + '    @vault_sat_name               : ' + COALESCE(@vault_sat_name, 'NULL')
						+ @NEW_LINE + '    @vault_temp_table_name        : ' + COALESCE(@vault_temp_table_name, 'NULL')
						+ @NEW_LINE + '    @vault_source_load_type       : ' + COALESCE(@vault_source_load_type, 'NULL')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF isnull(@vault_source_load_type, 'Full') not in ('Full', 'Delta')
			RAISERROR('Invalid Load Type: %s', 16, 1, @vault_source_load_type);
--IF isnull(@recreate_flag, '') not in ('Y', 'N') 
--			RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'
select
-- Global Defaults
 @def_global_lowdate				= cast([dbo].[fn_get_default_value] ('LowDate','Global')				as datetime)			
,@def_global_highdate				= cast([dbo].[fn_get_default_value] ('HighDate','Global')				as datetime)	
,@def_global_default_load_date_time	= cast([dbo].[fn_get_default_value] ('DefaultLoadDateTime','Global')	as varchar(128))
,@def_global_failed_lookup_key		= cast([dbo].[fn_get_default_value] ('FailedLookupKey', 'Global')     as integer)
-- Hub Defaults								
,@def_hub_prefix					= cast([dbo].[fn_get_default_value] ('prefix','hub')					as varchar(128))	
,@def_hub_schema					= cast([dbo].[fn_get_default_value] ('schema','hub')					as varchar(128))	
,@def_hub_filegroup					= cast([dbo].[fn_get_default_value] ('filegroup','hub')				as varchar(128))	
-- Link Defaults																						
,@def_link_prefix					= cast([dbo].[fn_get_default_value] ('prefix','lnk')					as varchar(128))	
,@def_link_schema					= cast([dbo].[fn_get_default_value] ('schema','lnk')					as varchar(128))	
,@def_link_filegroup				= cast([dbo].[fn_get_default_value] ('filegroup','lnk')				as varchar(128))	
-- Sat Defaults																							
,@def_sat_prefix					= cast([dbo].[fn_get_default_value] ('prefix','sat')					as varchar(128))	
,@def_sat_schema					= cast([dbo].[fn_get_default_value] ('schema','sat')					as varchar(128))	
,@def_sat_filegroup					= cast([dbo].[fn_get_default_value] ('filegroup','sat')				as varchar(128))

select @sat_start_date_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Version_Start_Date'
select @sat_end_date_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Version_End_Date'
select @sat_current_row_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Current_Row'

select @sat_source_date_time = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Source_Date_Time'

-- Object Specific Settings
-- Source Table

select 	 @source_system				= s.[source_system_name]	
        ,@source_database			= s.[timevault_name]
		,@source_schema				= t.[source_table_schema]
		,@source_table				= t.[source_table_name]
		,@source_table_config_key	= t.[source_table_key]
		,@source_qualified_name		= quotename(s.[timevault_name]) + '.' + quotename(t.[source_table_schema]) + '.' + quotename(t.[source_table_name])
		,@source_load_type			= coalesce(@vault_source_load_type, t.[source_table_load_type]) --The Run Time Load Type, If provided, overides the Default for the Source Table
from [dbo].[dv_source_system] s
inner join [dbo].[dv_source_table] t
on t.system_key = s.[source_system_key]
where 1=1
and s.[source_system_name]		= @vault_source_system_name
and t.[source_table_schema]		= @vault_source_table_schema
and t.[source_table_name]		= @vault_source_table_name

-- Get Satellite Details
select 	 @sat_database			= sat.[satellite_database]						
		,@sat_schema			= coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')		
		,@sat_table				= sat.[satellite_name]		
		,@sat_surrogate_keyname	= [dbo].[fn_get_object_name] (sat.[satellite_name],'SatSurrogate')		
		,@sat_config_key		= sat.[satellite_key]		
		,@sat_link_hub_flag		= sat.[link_hub_satellite_flag]		
		,@sat_qualified_name	= quotename(sat.[satellite_database]) + '.' + quotename(coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] (sat.[satellite_name], 'sat')))       
from [dbo].[dv_source_table] t
inner join [dbo].[dv_column] c
on c.table_key = t.[source_table_key]
inner join [dbo].[dv_satellite_column] sc
on sc.column_key = c.column_key
inner join [dbo].[dv_satellite] sat
on sat.satellite_key = sc.satellite_key
where 1=1
and t.[source_table_key] = @source_table_config_key
and sat.[satellite_name] = @vault_sat_name

-- Owner Hub Table

if @sat_link_hub_flag = 'H' 
	select   @hub_database			= h.[hub_database]
	        ,@hub_schema			= coalesce([hub_schema], @def_hub_schema, 'dbo')				
			,@hub_table				= h.[hub_name]
--			,@hub_surrogate_keyname = [dbo].[fn_get_object_name] ([dbo].[fn_get_object_name] ([hub_name], 'hub'),'HubSurrogate')
			,@hub_surrogate_keyname = (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](h.[hub_name], 'hub'))
			,@hub_config_key		= h.[hub_key]
			,@hub_qualified_name	= quotename([hub_database]) + '.' + quotename(coalesce([hub_schema], @def_hub_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([hub_name], 'hub')))	
	from [dbo].[dv_satellite] s
	inner join [dbo].[dv_hub] h
	on s.hub_key = h.hub_key
where 1=1
and s.[satellite_key] = @sat_config_key	
		
-- Owner Link Table
if @sat_link_hub_flag = 'L' 
begin
	select   @link_database			= l.[link_database]
	        ,@link_schema			= coalesce(l.[link_schema], @def_link_schema, 'dbo')				
			,@link_table			= l.[link_name]
			--,@link_surrogate_keyname = [dbo].[fn_get_object_name] ([dbo].[fn_get_object_name] ([link_name], 'lnk'),'LnkSurrogate')
			,@link_surrogate_keyname= (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](l.[link_name], 'lnk'))
			,@link_config_key		= l.[link_key]
			,@link_qualified_name	= quotename([link_database]) + '.' + quotename(coalesce(l.[link_schema], @def_link_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([link_name], 'lnk')))
	from [dbo].[dv_satellite] s
	inner join [dbo].[dv_link] l
	on s.link_key = l.link_key
    where 1=1
    and s.[satellite_key] = @sat_config_key
    
	set @link_lookup_joins = ''	
	set @link_hub_keys = ''	
end

select @source_load_date_time = 'vault_load_time'	

-- Build the Source Payload NB - needs to join to the Sat Table to get each satellite related to the source.
set @sql = ''
select @sql += 'src.' +quotename(sc.[column_name]) + @crlf +', '      
from [dbo].[dv_column] c
inner join dv_Satellite_Column sc
on c.column_key = sc.column_key
where 1=1
and [discard_flag] <> 1
and [table_key] = @source_table_config_key
and sc.[satellite_key] = @sat_config_key
order by sc.satellite_ordinal_position

select @source_payload = left(@sql, len(@sql) -1)

---- Build the Sat Payload 
set @sql = ''
select @sql += 'sat.' +quotename([column_name]) + @crlf +', ' 
from [dbo].[dv_default_column] 
where 1=1
and object_column_type <> 'Object_Key'
and [object_type] = 'Sat'
order by [ordinal_position]
set @sat_technical_columns = @sql

set @sql = ''
select @sql += 'sat.' +quotename(sc.[column_name]) + @crlf +', '
from [dbo].[dv_satellite] s
inner join [dbo].[dv_satellite_column] sc
on s.[satellite_key] = sc.[satellite_key]
inner join [dbo].[dv_column] c
on c.column_key = sc.column_key
where 1=1
and [discard_flag] <> 1
and s.[satellite_key] = @sat_config_key
order by sc.satellite_ordinal_position
select @sat_payload = left(@sql, len(@sql) -1)	


-- Compile the SQL

set @sql2 = ''

-- Insert New Rows for Updates
set @sql2 = 'BEGIN TRANSACTION' + @crlf
set @sql2 += 'INSERT INTO '	+ @sat_qualified_name + @crlf 
set @sql2 += ' (' + case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) else quotename(@link_surrogate_keyname) end + @crlf
set @sql2 += ',   ' + replace(@sat_technical_columns, 'sat.', '')
set @sql2 += replace(@sat_payload, 'sat.', '')
set @sql2 += ')' + @crlf
--set @sql2 += 'SELECT ' + @crlf + '  ' + case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) else quotename(@link_surrogate_keyname) end + @crlf   
set @sql2 += 'SELECT ' + @crlf + '  hl_driver_key' + @crlf											-- Driving Hub / Link Surrogate Key
--set @sql2 += ', '	+ @source_load_date_time + @crlf												-- Source Load Date Time
--set @sql2 += ', '	+ @sat_load_date_time + @crlf													-- Source Load Date Time
set @sql2 += ', case when MergeOutput.' + 
			case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) 
			else quotename(@link_surrogate_keyname) end + 
			' is null then ' + @sat_end_date_col +  ' else ' + @source_load_date_time + ' end'+@crlf-- Source Load Date Time. Deletes detected by the fact that they have no source Key
set @sql2 += ', '	+ '''' + cast(@source_table_config_key as varchar(128)) + '''' + @crlf			-- Source Table Reference Key
--set @sql2 += ', '	+ '1' + @crlf																	-- make the row Current
set @sql2 += ', case when MergeOutput.' + 
			case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) 
			else quotename(@link_surrogate_keyname) end + 
			' is null then 0 else 1 end' + @crlf													-- make the row Current if it's an update. Not Current if a Delete.
set @sql2 += ', case when MergeOutput.' + 
			case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) 
			else quotename(@link_surrogate_keyname) end + 
			' is null then 1 else 0 end' + @crlf													-- If it is a delete tombstone, set the deleted row flag. Deletes detected by the fact that they have no source Key
set @sql2 += ', MergeOutput.' + @sat_end_date_col + @crlf											-- Row Start Date						
--set @sql2 += ', '''	+ cast(@def_global_highdate as varchar(50)) + '''' + @crlf						-- Row End Date
set @sql2 += ', case when MergeOutput.' + 
			case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) 
			else quotename(@link_surrogate_keyname) end + 
			' is null then MergeOutput.' + @sat_end_date_col + ' else '''	+ cast(@def_global_highdate as varchar(50)) + ''' end' + @crlf -- Row End Date

set @sql2 += ', '	+ replace(@sat_payload, 'sat.', '')												-- Payload
set @sql2 += 'FROM ' + @crlf + ' (MERGE '  + @sat_qualified_name + ' WITH (HOLDLOCK) AS [sat]' + @crlf 
set @sql2 += ' USING ' + @vault_temp_table_name + ' AS [src]' + @crlf
set @sql2 += ' ON sat.' + case when @sat_link_hub_flag = 'H' then  @hub_surrogate_keyname else @link_surrogate_keyname end + ' = src.' +  case when @sat_link_hub_flag = 'H' then  @hub_surrogate_keyname else @link_surrogate_keyname end + @crlf
set @sql2 += ' AND sat.' + @sat_current_row_col + ' = 1' + @crlf

-- End Date Rows for Updates
set @sql2 += ' WHEN MATCHED AND EXISTS ' + @crlf
set @sql2 += '  (SELECT ' + @crlf
set @sql2 += '  src.' + case when @sat_link_hub_flag = 'H' then  @hub_surrogate_keyname else @link_surrogate_keyname end + @crlf + ', '
set @sql2 += @source_payload 
set @sql2 += ' EXCEPT ' + @crlf + ' SELECT ' + @crlf
set @sql2 += '  sat.' + case when @sat_link_hub_flag = 'H' then  @hub_surrogate_keyname else @link_surrogate_keyname end + @crlf + ', '
set @sql2 += @sat_payload
set @sql2 += ')' + @crlf + 'THEN UPDATE SET' + @crlf
set @sql2 += @sat_current_row_col + '  = 0' + @crlf
--set @sql2 = @sql2 + ',  ' + @sat_end_date_col + ' = iif([vault_load_time] > sat.' + @sat_start_date_col + ', [vault_load_time], dateadd(ms,1, sat.' + @sat_end_date_col + '))' + @crlf
set @sql2 += ',  ' + @sat_end_date_col + ' = iif(@version_date > sat.' + @sat_start_date_col + ', @version_date, dateadd(ms,1, sat.' + @sat_start_date_col + '))' + @crlf
--Insert New Rows
set @sql2 += 'WHEN NOT MATCHED BY TARGET ' + @crlf
set @sql2 += '  THEN INSERT ( ' + @crlf
set @sql2 += '  ' + case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) else quotename(@link_surrogate_keyname) end + @crlf
set @sql2 += ', ' + replace(@sat_technical_columns, 'sat.', '')
set @sql2 += replace(@sat_payload, 'sat.', '') + ')' + @crlf
set @sql2 += 'VALUES(' + @crlf + '  src.' + case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) else quotename(@link_surrogate_keyname) end + @crlf  + ', ' 
set @sql2 += '[vault_load_time]' + @crlf
set @sql2 += ', ' + cast(@source_table_config_key as varchar(50)) + @crlf
set @sql2 += ', 1' + @crlf
set @sql2 += ', 0' + @crlf
set @sql2 += ', @version_date ' + @crlf
set @sql2 += ', ''' + cast(@def_global_highdate as varchar(50)) + '''' + @crlf
set @sql2 += ', ' +@source_payload + ')' + @crlf
-- End Date Deleted Rows
if @source_load_type = 'Full'
begin
	set @sql2 += 'WHEN NOT MATCHED BY SOURCE AND sat.' + @sat_current_row_col + ' = 1' +@crlf
	set @sql2 += 'THEN UPDATE SET ' + @crlf
	set @sql2 += '  ' + @sat_current_row_col + ' = 0' + @crlf
	set @sql2 += ', ' + @sat_end_date_col + ' = iif(@version_date > sat.' + @sat_start_date_col + ', @version_date, dateadd(ms,1, sat.' + @sat_end_date_col + ')'+ @crlf
	set @sql2 += ')' + @crlf 
end
-- Output End Dated Rows for Insert by the Outer Query. 
-- Also output the End Date, for use as the next start date.
set @sql2 += '  OUTPUT $action AS Action' + @crlf
set @sql2 += '        ,inserted.' + @sat_end_date_col + @crlf
set @sql2 += '        ,inserted.' + @sat_current_row_col + @crlf  --------------------------------
set @sql2 += '        ,inserted.' + case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) else quotename(@link_surrogate_keyname) end + ' as hl_driver_key' + @crlf
set @sql2 += '        ,inserted.' + @sat_source_date_time + @crlf
set @sql2 += '        ,[src].*' + @crlf
set @sql2 += ') AS MergeOutput' + @crlf
set @sql2 += '  WHERE 1=1' + @crlf
set @sql2 += '  AND MergeOutput.Action = ''UPDATE''' + @crlf
--set @sql2 += '    AND ' + case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) else quotename(@link_surrogate_keyname) end + ' is not null' + @crlf
set @sql2 += ';' + @crlf-- Merge Statement Must end with ';'

-- Log Completion
set @sql2 += 'EXECUTE [dv_log].[dv_log_progress] ''sat'',''' + @sat_table + ''',''' + @sat_schema + ''',''' +  @sat_database + ''',' --+ @crlf
set @sql2 += '''' + @source_table + ''',''' +  @source_schema + ''',''' + @source_system + ''',' --+ @crlf 
set @sql2 += cast(isnull(@execution_id, 0) as varchar(50)) + ', @version_date, '  
          + cast(isnull(@rows_inserted, 0) as varchar(50)) + ',' + cast(isnull(@rows_updated, 0) as varchar(50)) + ',' + cast(isnull(@rows_deleted, 0) as varchar(50)) + @crlf
set @sql2 += 'COMMIT;' + @crlf
select @vault_sql_statement = @sql2
IF @_JournalOnOff = 'ON' SET @_ProgressText = @crlf + @vault_sql_statement + @crlf
/*--------------------------------------------------------------------------------------------------------------*/
--SET @_Step = 'Load The ' + case when @sat_link_hub_flag = 'H' then 'Hub' else 'Link' end
--IF @_JournalOnOff = 'ON'
--	SET @_ProgressText += @sql
--print @vault_sql_statement
--select @vault_sql_statement
--EXECUTE sp_executesql @SQL;
/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Loaded Object: ' + @sat_qualified_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Load Object: ' + @sat_qualified_name
IF (XACT_STATE() = -1) -- uncommitable transaction
OR (@@TRANCOUNT > 0 AND XACT_STATE() != 1) -- undocumented uncommitable transaction
	BEGIN
		ROLLBACK TRAN;
		SET @_ErrorContext = @_ErrorContext + ' (Forced rolled back of all changes)';
	END
	
EXEC log4.ExceptionHandler
		  @ErrorContext  = @_ErrorContext
		, @ErrorNumber   = @_Error OUT
		, @ReturnMessage = @_Message OUT
		, @ExceptionId   = @_ExceptionId OUT
;
END CATCH

--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	--! Clean up

	--!
	--! Use dbo.udf_FormatElapsedTime() to get a nicely formatted run time string e.g.
	--! "0 hr(s) 1 min(s) and 22 sec(s)" or "1345 milliseconds"
	--!
	IF @_Error = 0
		BEGIN
			SET @_Step			= 'OnComplete'
			SET @_Severity		= @SEVERITY_SUCCESS
			SET @_Message		= COALESCE(@_Message, @_Step)
								+ ' in a total run time of ' + log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
			SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
		END
	ELSE
		BEGIN
			SET @_Step			= COALESCE(@_Step, 'OnError')
			SET @_Severity		= @SEVERITY_SEVERE
			SET @_Message		= COALESCE(@_Message, @_Step)
								+ ' after a total run time of ' + log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
			SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
		END

	IF @_JournalOnOff = 'ON'
		EXEC log4.JournalWriter
				  @Task				= @_FunctionName
				, @FunctionName		= @_FunctionName
				, @StepInFunction	= @_Step
				, @MessageText		= @_Message
				, @Severity			= @_Severity
				, @ExceptionId		= @_ExceptionId
				--! Supply all the progress info after we've gone to such trouble to collect it
				, @ExtraInfo        = @_ProgressText

	--! Finally, throw an exception that will be detected by the caller
	IF @DoThrowError = 1 AND @_Error > 0
		RAISERROR(@_Message, 16, 99);

	SET NOCOUNT OFF;

	--! Return the value of @@ERROR (which will be zero on success)
	RETURN (@_Error);
END