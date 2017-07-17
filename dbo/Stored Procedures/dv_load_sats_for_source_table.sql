
CREATE PROCEDURE [dbo].[dv_load_sats_for_source_table]
(
  @vault_source_unique_name		varchar(128) = NULL
, @vault_source_load_type		varchar(50)  = NULL
, @vault_source_version_key		int			 = NULL
, @vault_runkey					int          = NULL
, @dogenerateerror				bit				= 0
, @dothrowerror					bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly
--         Add Transactionality for Multi Sat Loads
--         Pick a single sat to load (from a multi sat)
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

-- Object Specific Settings
-- Source Table
declare  @stage_database					varchar(128)
		,@stage_schema						varchar(128)
		,@stage_table						varchar(128)
		,@stage_table_config_key			int
		,@stage_source_version_key			int
		,@stage_qualified_name				varchar(512)
		,@stage_load_date_time				varchar(128)
		,@stage_payload						nvarchar(max)
		,@stage_source_date_time			varchar(128)
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
		,@sat_technical_columns				nvarchar(max)
		,@sat_payload						nvarchar(max)

--  Working Storage
DECLARE @sat_insert_count			int
       ,@temp_table_name			varchar(116)
	   ,@sql						nvarchar(max)
	   ,@sql1						nvarchar(max)
	   ,@sql2						nvarchar(max)
	   ,@sql3						nvarchar(max)
	   ,@surrogate_key_match        varchar(1000)
DECLARE @declare					nvarchar(512)	= ''
DECLARE @count_rows					nvarchar(256)	= ''
DECLARE @match_list					nvarchar(max)	= ''
DECLARE @value_list					nvarchar(max)	= ''
DECLARE @sat_column_list			nvarchar(max)	= ''
DECLARE @hub_column_list			nvarchar(max)	= ''
DECLARE @rc							int

DECLARE @ParmDefinition				nvarchar(500);

DECLARE @satellite_list				table (sat_database				varchar(128)				
										  ,sat_schema				varchar(128)			
										  ,sat_table				varchar(128)
										  ,sat_surrogate_keyname    varchar(128)
										  ,sat_config_key			int
										  ,sat_link_hub_flag		char(1)
										  ,sat_qualified_name		varchar(512)
										  )

DECLARE @wrk_link_joins			nvarchar(max)
DECLARE @wrk_link_keys			nvarchar(max)
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
						+ @NEW_LINE + '    @vault_source_unique_name     : ' + COALESCE(@vault_source_unique_name, 'NULL')
						+ @NEW_LINE + '    @vault_source_load_type       : ' + COALESCE(@vault_source_load_type, 'NULL')
						+ @NEW_LINE + '    @vault_runkey                 : ' + COALESCE(CAST(@vault_runkey AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY

SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF isnull(@vault_source_load_type, '') not in ('Full', 'Delta')
			RAISERROR('Invalid Load Type: %s', 16, 1, @vault_source_load_type);
IF ((@vault_runkey is not null) and ((select count(*) from [dv_scheduler].[dv_run] where @vault_runkey = [run_key]) <> 1))
			RAISERROR('Invalid @vault_runkey provided: %i', 16, 1, @vault_runkey);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'
-- System Wide Defaults
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

-- Object Specific Settings
-- Source Table
-- find out if a "source_date_time" column has been supplied.
select @stage_source_date_time = [column_name]
from [dbo].[dv_source_table] st
inner join [dbo].[dv_column] c
on st.[source_table_key] = c.table_key
where 1=1
and st.[source_unique_name] = @vault_source_unique_name
and c.[is_source_date] = 1
if @stage_source_date_time is NULL 
	set @stage_source_date_time = 'SYSDATETIMEOFFSET()'
	else 
	set @stage_source_date_time = '[vault_load_time]' 
-- get source table details:
select 	 @stage_database			= sdb.[stage_database_name]
		,@stage_schema				= ss.[stage_schema_name]
		,@stage_table				= st.[stage_table_name]
		,@stage_table_config_key	= st.[source_table_key]
		,@stage_source_version_key	= isnull(@vault_source_version_key, sv.source_version_key) -- if no source version is provided, use the current source version for the source table used as source for this load.
		,@stage_qualified_name		= quotename(sdb.[stage_database_name]) + '.' + quotename(ss.[stage_schema_name]) + '.' + quotename(st.[stage_table_name])
from [dbo].[dv_source_table] st
inner join [dbo].[dv_stage_schema] ss on ss.stage_schema_key = st.stage_schema_key
inner join [dbo].[dv_stage_database] sdb on sdb.stage_database_key = ss.stage_database_key
left join  [dbo].[dv_source_version] sv on sv.source_table_key = st.source_table_key	
									   and sv.is_current= 1
where 1=1
and st.[source_unique_name]		= @vault_source_unique_name

if @@ROWCOUNT <> 1 RAISERROR ('Invalid Link Parameters Supplied',16,1);
select @rc = count(*) from [dbo].[dv_source_version] where source_version_key = @stage_source_version_key and is_current= 1
if @rc <> 1 RAISERROR('dv_source_table or current dv_source_version missing for: %s, source version : %i', 16, 1, @stage_qualified_name, @stage_source_version_key);


-- Get a list of Satellites
insert @satellite_list
select distinct 
       sat.[satellite_database]
      ,coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')
	  ,sat.[satellite_name]
	  ,[dbo].[fn_get_object_name] (sat.[satellite_name],'SatSurrogate') 
	  ,sat.[satellite_key] 
	  ,sat.[link_hub_satellite_flag]
	  ,quotename(sat.[satellite_database]) + '.' + quotename(coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] (sat.[satellite_name], 'sat')))       
from [dbo].[dv_source_table] t
inner join [dbo].[dv_column] c
on c.table_key = t.[source_table_key]
inner join [dbo].[dv_satellite_column] sc
on sc.satellite_col_key = c.satellite_col_key
inner join [dbo].[dv_satellite] sat
on sat.satellite_key = sc.satellite_key
where 1=1
and t.[source_table_key] = @stage_table_config_key

-- Note that split satellites can only be of 1 type - Link or Hub
if (select count(distinct [sat_link_hub_flag]) from @satellite_list) <> 1 RAISERROR('Multiple Satellites in this load are mixed between Hubs and Links. This is Invalid', 16, 1); 
select @sat_link_hub_flag = [sat_link_hub_flag] from @satellite_list 

if (select count(distinct [sat_database]) from @satellite_list) <> 1 RAISERROR('Multiple Databases are in this load. This is Invalid', 16, 1); 
select @sat_database = [sat_database] from @satellite_list 

-- Owner Hub Table

if @sat_link_hub_flag = 'H' 
	select   @hub_database			= h.[hub_database]
	        ,@hub_schema			= coalesce([hub_schema], @def_hub_schema, 'dbo')				
			,@hub_table				= h.[hub_name]
			,@hub_surrogate_keyname = [dbo].[fn_get_object_name] ([dbo].[fn_get_object_name] ([hub_name], 'hub'),'HubSurrogate')
			,@hub_config_key		= h.[hub_key]
			,@hub_qualified_name	= quotename([hub_database]) + '.' + quotename(coalesce([hub_schema], @def_hub_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([hub_name], 'hub')))	
	from [dbo].[dv_satellite] s
	inner join [dbo].[dv_hub] h
	on s.hub_key = h.hub_key
where 1=1
and s.[satellite_key] = (select top 1 sat_config_key from @satellite_list) 	
		
-- Owner Link Table
if @sat_link_hub_flag = 'L' 
	select   @link_database			= l.[link_database]
	        ,@link_schema			= coalesce(l.[link_schema], @def_link_schema, 'dbo')				
			,@link_table			= l.[link_name]
			,@link_surrogate_keyname = [dbo].[fn_get_object_name] ([dbo].[fn_get_object_name] ([link_name], 'lnk'),'LnkSurrogate')
			,@link_config_key		= l.[link_key]
			,@link_qualified_name	= quotename([link_database]) + '.' + quotename(coalesce(l.[link_schema], @def_link_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([link_name], 'lnk')))
	from [dbo].[dv_satellite] s
	inner join [dbo].[dv_link] l
	on s.link_key = l.link_key
    where 1=1
    and s.[satellite_key] = (select top 1 sat_config_key from @satellite_list)
 
 -- Get the SQL for the Key Lookup   

EXECUTE [dbo].[dv_load_source_table_key_lookup] @vault_source_unique_name, 'N', @vault_source_load_type, @temp_table_name OUTPUT, @sql1 OUTPUT

-- Now Get the Satellite Update SQL

--set @sql2 =			'DECLARE @version_date_char varchar(20)' + @crlf
set @sql2 = ''

set @sql2 = @sql2 + 'SELECT @__vault_runkey = ' + ISNULL(CAST(@vault_runkey as varchar(20)), 0) + @crlf
set @sql2 = @sql2 + 'SELECT @version_date = SYSDATETIMEOFFSET()'  + @crlf 
set @sql2 = @sql2 + 'SELECT TOP 1 @source_date_time = ' + @stage_source_date_time + ' FROM ' + @temp_table_name + @crlf
--print @sql2
--set @sql2 = @sql2 + 'select @version_date_char = CONVERT(varchar(50), @version_date) '  + @crlf 
DECLARE c_sat_list CURSOR FOR 
select sat_table
  FROM @satellite_list

OPEN c_sat_list   
FETCH NEXT FROM c_sat_list 
INTO @sat_table		 

WHILE @@FETCH_STATUS = 0   
BEGIN
EXECUTE [dbo].[dv_load_sat_table] @vault_source_unique_name, @sat_table, @temp_table_name, @vault_source_load_type, @stage_source_version_key, @sql OUTPUT, @vault_runkey
print @sat_table
--print @sql
set @sql2 += @sql
FETCH NEXT FROM c_sat_list 
INTO @sat_table	
END   

CLOSE c_sat_list   
DEALLOCATE c_sat_list

set @sql = @sql1 + @sql2 

--/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Load The Source into Sat(s)'
IF @_JournalOnOff = 'ON' SET @_ProgressText += @SQL
--print @SQL
EXECUTE(@SQL);
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