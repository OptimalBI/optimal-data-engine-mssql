
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
-- Local Defaults Values
DECLARE @crlf								char(2)			= CHAR(13) + CHAR(10)
-- Global Defaults
DECLARE  
		 @def_global_lowdate				datetime
        ,@def_global_highdate				datetime
        ,@def_global_default_load_date_time	varchar(128) 
		,@def_global_failed_lookup_key		int
		,@def_sat_hashmatching_type			varchar(10)
		,@def_hl_surrogate_col_type			varchar(128)

--Sat Defaults									
		,@def_sat_prefix					varchar(128)
		,@def_sat_schema					varchar(128)
		,@sat_start_date_col				varchar(128)
		,@sat_end_date_col					varchar(128)
		,@sat_current_row_col				varchar(128)
		,@sat_hashmatching_type				varchar(10)	
		,@sat_hashmatching_col				varchar(128)
		,@default_columns					[dbo].[dv_column_type]				

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
		,@hub_surrogate_keyname				varchar(128)
-- Link Table
		,@link_surrogate_keyname			varchar(128)
-- Sat Table
		,@sat_database						varchar(128)
		,@sat_schema						varchar(128)
		,@sat_table							varchar(128)
		,@sat_surrogate_keyname				varchar(128)
		,@sat_config_key					int
		,@sat_link_hub_flag					char(1)
		,@sat_qualified_name				varchar(512)
		,@sat_tombstone_indicator			varchar(128)
		,@sat_source_date_time				varchar(50)
		,@sat_hl_surrogate_col              varchar(128)
		,@sat_technical_columns				nvarchar(max)
		,@sat_payload						nvarchar(max)
		,@matching_payload					nvarchar(max)

--  Working Storage
DECLARE @execution_id				int
	   ,@sql						nvarchar(max)
	   ,@sql1						nvarchar(max)
	   ,@payload_columns			[dbo].[dv_column_type]
	   
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
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'
select
-- Global Defaults
 @def_global_lowdate				= cast([dbo].[fn_get_default_value] ('LowDate','Global')				as datetime)			
,@def_global_highdate				= cast([dbo].[fn_get_default_value] ('HighDate','Global')				as datetime)	
,@def_global_default_load_date_time	= cast([dbo].[fn_get_default_value] ('DefaultLoadDateTime','Global')	as varchar(128))
,@def_global_failed_lookup_key		= cast([dbo].[fn_get_default_value] ('FailedLookupKey', 'Global')     as integer)

-- Sat Defaults																							
,@def_sat_prefix					= cast([dbo].[fn_get_default_value] ('prefix','sat')					as varchar(128))	
,@def_sat_schema					= cast([dbo].[fn_get_default_value] ('schema','sat')					as varchar(128))	
,@def_sat_hashmatching_type         = cast([dbo].[fn_get_default_value] ('HashMatchingType','sat')		    as varchar) 

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

select @sat_tombstone_indicator = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Tombstone_Indicator'

select @source_load_date_time = 'vault_load_time'

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
		,@sat_surrogate_keyname	= (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](sat.[satellite_name], 'sat'))		
		,@sat_config_key		= sat.[satellite_key]		
		,@sat_link_hub_flag		= sat.[link_hub_satellite_flag]
		,@sat_hashmatching_type = sat.hashmatching_type		
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


if coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type, 'None') <> 'None'
	select @sat_hashmatching_col = [column_name]		   
	from [dbo].[dv_default_column]
	where 1=1
	and [object_type] = 'Sat'
	and [object_column_type] <> 'Object_Key' 
	and [object_column_type] = coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type) + '_match'

if @sat_link_hub_flag = 'H'	
SELECT @def_hl_surrogate_col_type = [dbo].[fn_build_column_definition] ([column_type], [column_length], [column_precision], [column_scale], [collation_Name], 1, 0)
  FROM  [dbo].[dv_default_column]
  where [object_column_type] = 'Object_Key'
  and object_type = 'Hub' 
if @sat_link_hub_flag = 'L'	
SELECT @def_hl_surrogate_col_type = [dbo].[fn_build_column_definition] ([column_type], [column_length], [column_precision], [column_scale], [collation_Name], 1, 0)
  FROM  [dbo].[dv_default_column]
  where [object_column_type] = 'Object_Key'
  and object_type = 'Lnk'

-- Owner Hub Table
if @sat_link_hub_flag = 'H' 
	select @hub_surrogate_keyname = (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](h.[hub_name], 'hub'))
	from [dbo].[dv_satellite] s
	inner join [dbo].[dv_hub] h
	on s.hub_key = h.hub_key
	where 1=1
	and s.[satellite_key] = @sat_config_key	
		
-- Owner Link Table
if @sat_link_hub_flag = 'L' 
	select @link_surrogate_keyname= (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](l.[link_name], 'lnk'))
	from [dbo].[dv_satellite] s
	inner join [dbo].[dv_link] l
	on s.link_key = l.link_key
    where 1=1
    and s.[satellite_key] = @sat_config_key 

select @sat_hl_surrogate_col = case when @sat_link_hub_flag = 'H' then  quotename(@hub_surrogate_keyname) else quotename(@link_surrogate_keyname) end


-- Build the Source Payload NB - needs to join to the Sat Table to get each satellite related to the source.
set @sql = ''
if coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type, 'None') <> 'None'
 select @sql += 'src.[vault_hashdiff]' + @crlf +', '
select @sql += 'src.' +quotename([column_name]) + @crlf +', '      
from [dbo].[dv_column] c
inner join dv_Satellite_Column sc
on c.column_key = sc.column_key
where 1=1
and [discard_flag] <> 1
and [table_key] = @source_table_config_key
and sc.[satellite_key] = @sat_config_key
order by c.satellite_ordinal_position
select @source_payload = left(@sql, len(@sql) -1)

-- Build the Sat Payload
insert @default_columns	
select  [column_name]
       ,[column_type]
       ,[column_length]
	   ,[column_precision]
	   ,[column_scale]
	   ,[collation_Name]
	   ,-1
       ,[ordinal_position]
	   ,-1
	   ,''
	   ,''
from [dbo].[dv_default_column]
where 1=1
and [object_type] = 'Sat'
and [object_column_type] <> 'Object_Key' 
and [object_column_type] not like '%_match'

set @sql = ''
select @sql += 'sat.' +quotename([column_name]) + @crlf +', ' 
from @default_columns
where 1=1
order by [source_ordinal_position]
if coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type, 'None') <> 'None'
 select @sql += 'sat.' + quotename(@sat_hashmatching_col) + @crlf +', ' 
set @sat_technical_columns = @sql

insert @payload_columns
select  c.[column_name]
       ,c.[column_type]
       ,c.[column_length]
	   ,c.[column_precision]
	   ,c.[column_scale]
	   ,c.[collation_Name]
	   ,c.[bk_ordinal_position]
       ,c.[source_ordinal_position]
	   ,c.[satellite_ordinal_position]
	   ,''
	   ,''
  FROM [dbo].[dv_satellite] s
  inner join [dbo].[dv_satellite_column] hc
  on s.satellite_key = hc.satellite_key
  inner join [dbo].[dv_column] c
  on hc.column_key = c.column_key
  where 1=1
  and s.[satellite_key] = @sat_config_key
  and isnull(c.discard_flag, 0) <> 1 

set @sql = ''
select @sql += 'sat.' +quotename([column_name]) + @crlf +', '
from @payload_columns s
order by s.satellite_ordinal_position
select @sat_payload = left(@sql, len(@sql) -1)	

if coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type, 'None') = 'None'
	begin
	set @sql = ''
	select @sql += '     OR (CASE WHEN (src.' + quotename([column_name]) + ' IS NULL AND sat.' + quotename([column_name]) + ' IS NULL) OR src.' + quotename([column_name]) + ' = sat.' + quotename([column_name]) + '  THEN 1 ELSE 0 END) = 0' + @crlf 
	from @payload_columns s
	order by s.satellite_ordinal_position
	select @matching_payload = @sql --right(@sql, len(@sql) - 7)
	end

-- Compile the SQL

set @sql1 = ''

-- Insert New Rows for Updates
set @sql1 = 'BEGIN TRANSACTION' + @crlf
set @sql1 += 'declare @ChangeKeys table(ChangeKey ' + @def_hl_surrogate_col_type + ')' + @crlf
set @sql1 += 'declare @insertcount bigint , @updatecount bigint , @deletecount bigint'
set @sql1 += '-- Change Rows ' + @crlf
set @sql1 += 'INSERT INTO '	+ @sat_qualified_name + @crlf 
set @sql1 += ' (' + @sat_hl_surrogate_col + @crlf 
set @sql1 += ', ' + replace(@sat_technical_columns, 'sat.', '')
set @sql1 += replace(@sat_payload, 'sat.', '')
set @sql1 += ')' + @crlf
set @sql1 += 'output inserted.' + @sat_surrogate_keyname + ' into @ChangeKeys' + @crlf
set @sql1 += 'SELECT ' + @crlf + '  src.' + @sat_hl_surrogate_col + @crlf  + ', ' 
set @sql1 += '[vault_load_time]' + @crlf
set @sql1 += ', ' + cast(@source_table_config_key as varchar(50)) + @crlf
set @sql1 += ', 1' + @crlf
set @sql1 += ', 0' + @crlf
set @sql1 += ', @version_date ' + @crlf
set @sql1 += ', ''' + cast(@def_global_highdate as varchar(50)) + '''' + @crlf
set @sql1 += ', ' + @source_payload 
set @sql1 += 'FROM ' + @vault_temp_table_name + ' src' + @crlf
set @sql1 += 'INNER JOIN ' + @sat_qualified_name + ' sat' + @crlf 
set @sql1 += 'ON sat.' + @sat_hl_surrogate_col  + ' = ' + 'src.' + @sat_hl_surrogate_col + @crlf
set @sql1 += 'WHERE ((CASE WHEN sat.' + @sat_tombstone_indicator + ' = 1 THEN 0 ELSE 1 END) = 0' + @crlf                                                   --*************************************************************************
if coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type, 'None') = 'None'
	set @sql1 += @matching_payload 
else
	set @sql1 += 'OR sat.' + @sat_hashmatching_col + ' <> src.[vault_hashdiff]' + @crlf
set @sql1 += ')' + @crlf
set @sql1 += 'AND sat.' + @sat_end_date_col + ' = ''' +  cast(@def_global_highdate as varchar) + '''' + @crlf
set @sql1 += 'select @updatecount = @@ROWCOUNT;' + @crlf

set @sql1 += @crlf + '-- New Rows ' + @crlf
set @sql1 += 'INSERT INTO '	+ @sat_qualified_name + @crlf 
set @sql1 += ' (' + @sat_hl_surrogate_col + @crlf
set @sql1 += ', ' + replace(@sat_technical_columns, 'sat.', '')
set @sql1 += replace(@sat_payload, 'sat.', '')
set @sql1 += ')' + @crlf
set @sql1 += 'SELECT ' + @crlf + '  src.' + @sat_hl_surrogate_col + @crlf  + ', ' 
set @sql1 += '[vault_load_time]' + @crlf
set @sql1 += ', ' + cast(@source_table_config_key as varchar(50)) + @crlf
set @sql1 += ', 1' + @crlf
set @sql1 += ', 0' + @crlf
set @sql1 += ', @version_date ' + @crlf
set @sql1 += ', ''' + cast(@def_global_highdate as varchar(50)) + '''' + @crlf
set @sql1 += ', ' + @source_payload 
set @sql1 += 'FROM ' + @vault_temp_table_name + ' src' + @crlf
set @sql1 += 'LEFT JOIN ' + @sat_qualified_name + ' sat' + @crlf 
set @sql1 += 'ON sat.' + @sat_hl_surrogate_col  + ' = ' + 'src.' + @sat_hl_surrogate_col + @crlf
set @sql1 += 'AND sat.' + @sat_end_date_col + ' = ''' +  cast(@def_global_highdate as varchar) + '''' + @crlf
set @sql1 += 'WHERE sat.' + @sat_hl_surrogate_col  + ' is null' + @crlf
set @sql1 += 'select @insertcount = @@ROWCOUNT;' + @crlf

set @sql1 += @crlf + '-- Delete Rows ' + @crlf
if @source_load_type = 'Full'
	begin
	set @sql1 += 'INSERT INTO '	+ @sat_qualified_name + @crlf 
	set @sql1 += ' (' + @sat_hl_surrogate_col + @crlf
	set @sql1 += ', ' + replace(left(@sat_technical_columns, len(@sat_technical_columns) - 3), 'sat.', '')
	set @sql1 += ')' + @crlf
	set @sql1 += 'output inserted.' + @sat_surrogate_keyname + ' into @ChangeKeys' + @crlf
	set @sql1 += 'SELECT ' + @crlf + '  sat.' + @sat_hl_surrogate_col + @crlf  + ', ' 
	set @sql1 += '[vault_load_time]' + @crlf
	set @sql1 += ', ' + cast(@source_table_config_key as varchar(50)) + @crlf
	set @sql1 += ', 1' + @crlf
	set @sql1 += ', 1' + @crlf -- Tombstone
	set @sql1 += ', @version_date ' + @crlf
	set @sql1 += ', ''' + cast(@def_global_highdate as varchar(50)) + '''' + @crlf
	if coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type, 'None') <> 'None'
		set @sql1 += ', ''<Tombstone>''' + @crlf
	set @sql1 += 'FROM ' + @vault_temp_table_name + ' src' + @crlf
	set @sql1 += 'RIGHT JOIN ' + @sat_qualified_name + ' sat' + @crlf 
	set @sql1 += 'ON sat.' + @sat_hl_surrogate_col  + ' = ' + 'src.' + @sat_hl_surrogate_col + @crlf
	set @sql1 += 'WHERE src.' + @sat_hl_surrogate_col + ' IS NULL' + @crlf
	set @sql1 += 'AND sat.' + @sat_end_date_col + ' = ''' +  cast(@def_global_highdate as varchar) + '''' + @crlf
	set @sql1 += 'AND sat.' + @sat_tombstone_indicator + ' = 0' + @crlf
	set @sql1 += 'select @deletecount = @@ROWCOUNT;' + @crlf
	end
else
	set @sql1 += 'select @deletecount = 0;' + @crlf

set @sql1 += @crlf + '-- End Dating Old Rows ' + @crlf
--set @sql1 += 'select ChangeKey from @ChangeKeys order by 1' + @crlf
set @sql1 += ';with wBaseset as(select ' + quotename(@sat_surrogate_keyname) + ' AS NewSurrogate, lag(' + quotename(@sat_surrogate_keyname) + ', 1) over (partition by ' + @sat_hl_surrogate_col + ' order by ' + @sat_start_date_col + ') AS  ChangeSurrogate, ' + @sat_start_date_col + @crlf
set @sql1 += 'FROM ' + @sat_qualified_name + @crlf
set @sql1 += ')' + @crlf
set @sql1 += 'update sat set sat.' + @sat_end_date_col + ' = wBaseSet.' + @sat_start_date_col + @crlf
set @sql1 += '    ,sat.' + @sat_current_row_col + ' = 0' + @crlf
set @sql1 += 'from ' + @sat_qualified_name + ' sat' + @crlf
set @sql1 += 'inner join wBaseset on sat.' + @sat_surrogate_keyname + ' = wBaseSet.ChangeSurrogate' + @crlf
set @sql1 += 'WHERE wBaseSet.NewSurrogate in(select ChangeKey from @ChangeKeys)' + @crlf


-- Log Completion
set @sql1 += @crlf + '-- Log Progress ' + @crlf
set @sql1 += 'EXECUTE [dv_log].[dv_log_progress] ''sat'',''' + @sat_table + ''',''' + @sat_schema + ''',''' +  @sat_database + ''',' --+ @crlf
set @sql1 += '''' + @source_table + ''',''' +  @source_schema + ''',''' + @source_system + ''',' --+ @crlf 
set @sql1 += cast(isnull(@execution_id, 0) as varchar(50)) + ', @version_date, @insertcount, @updatecount, @deletecount' + @crlf

set @sql1 += 'COMMIT;' + @crlf
select @vault_sql_statement = @sql1
IF @_JournalOnOff = 'ON' SET @_ProgressText = @crlf + @vault_sql_statement + @crlf
/*--------------------------------------------------------------------------------------------------------------*/
IF @_JournalOnOff = 'ON'
	SET @_ProgressText += @sql
select @vault_sql_statement

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