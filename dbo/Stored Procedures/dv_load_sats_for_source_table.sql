
CREATE PROCEDURE [dbo].[dv_load_sats_for_source_table]
(
  @vault_source_system_name		varchar(128) = NULL
, @vault_source_table_schema	varchar(128) = NULL
, @vault_source_table_name		varchar(128) = NULL
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
		,@source_system						varchar(128)
        ,@source_database					varchar(128)
		,@source_schema						varchar(128)
		,@source_table						varchar(128)
		,@source_table_config_key			int
		,@source_qualified_name				varchar(512)
		,@source_load_date_time				varchar(128)
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
		,@sat_technical_columns				nvarchar(max)
		,@sat_payload						nvarchar(max)

--  Working Storage
DECLARE @sat_insert_count			int
       ,@temp_table_name			varchar(116)
	   ,@sql						nvarchar(max)
	   ,@sql1						nvarchar(max)
	   ,@sql2						nvarchar(max)
	   ,@surrogate_key_match        varchar(1000)
DECLARE @declare					nvarchar(512)	= ''
DECLARE @count_rows					nvarchar(256)	= ''
DECLARE @match_list					nvarchar(max)	= ''
DECLARE @value_list					nvarchar(max)	= ''
DECLARE @sat_column_list			nvarchar(max)	= ''
DECLARE @hub_column_list			nvarchar(max)	= ''

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
DECLARE @wrk_link__keys			nvarchar(max)
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
						+ @NEW_LINE + '    @vault_source_system_name     : ' + COALESCE(@vault_source_system_name, 'NULL')
						+ @NEW_LINE + '    @vault_source_table_schema    : ' + COALESCE(@vault_source_table_schema, 'NULL')
						+ @NEW_LINE + '    @vault_source_table_name      : ' + COALESCE(@vault_source_table_name, 'NULL')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY

SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

--IF (select count(*) from [dbo].[dv_sat] where sat_name = @sat_name) <> 1
--			RAISERROR('Invalid sat Name: %s', 16, 1, @sat_name);
--IF isnull(@recreate_flag, '') not in ('Y', 'N') 
--			RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'
-- System Wide Defaults
select
-- Global Defaults
 @def_global_lowdate				= cast([dbo].[fn_GetDefaultValue] ('LowDate','Global')				as datetime)			
,@def_global_highdate				= cast([dbo].[fn_GetDefaultValue] ('HighDate','Global')				as datetime)	
,@def_global_default_load_date_time	= cast([dbo].[fn_GetDefaultValue] ('DefaultLoadDateTime','Global')	as varchar(128))
,@def_global_failed_lookup_key		= cast([dbo].[fn_GetDefaultValue] ('FailedLookupKey', 'Global')     as integer)
-- Hub Defaults								
,@def_hub_prefix					= cast([dbo].[fn_GetDefaultValue] ('prefix','hub')					as varchar(128))	
,@def_hub_schema					= cast([dbo].[fn_GetDefaultValue] ('schema','hub')					as varchar(128))	
,@def_hub_filegroup					= cast([dbo].[fn_GetDefaultValue] ('filegroup','hub')				as varchar(128))	
-- Link Defaults																						
,@def_link_prefix					= cast([dbo].[fn_GetDefaultValue] ('prefix','lnk')					as varchar(128))	
,@def_link_schema					= cast([dbo].[fn_GetDefaultValue] ('schema','lnk')					as varchar(128))	
,@def_link_filegroup				= cast([dbo].[fn_GetDefaultValue] ('filegroup','lnk')				as varchar(128))	
-- Sat Defaults																							
,@def_sat_prefix					= cast([dbo].[fn_GetDefaultValue] ('prefix','sat')					as varchar(128))	
,@def_sat_schema					= cast([dbo].[fn_GetDefaultValue] ('schema','sat')					as varchar(128))	
,@def_sat_filegroup					= cast([dbo].[fn_GetDefaultValue] ('filegroup','sat')				as varchar(128))

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
select 	 @source_system				= s.[source_system_name]	
        ,@source_database			= s.[timevault_name]
		,@source_schema				= t.[source_table_schema]
		,@source_table				= t.[source_table_name]
		,@source_table_config_key	= t.[table_key]
		,@source_qualified_name		= quotename(s.[timevault_name]) + '.' + quotename(t.[source_table_schema]) + '.' + quotename(t.[source_table_name])
from [dbo].[dv_source_system] s
inner join [dbo].[dv_source_table] t
on t.system_key = s.system_key
where 1=1
and s.[source_system_name]		= @vault_source_system_name
and t.[source_table_schema]		= @vault_source_table_schema
and t.[source_table_name]		= @vault_source_table_name

-- Get a list of Satellites
insert @satellite_list
select distinct 
       sat.[satellite_database]
      ,coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')
	  ,sat.[satellite_name]
	  ,[dbo].[fn_GetObjectName] (sat.[satellite_name],'SatSurrogate') 
	  ,sat.[satellite_key] 
	  ,sat.[link_hub_satellite_flag]
	  ,quotename(sat.[satellite_database]) + '.' + quotename(coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_GetObjectName] (sat.[satellite_name], 'sat')))       
from [dbo].[dv_source_table] t
inner join [dbo].[dv_column] c
on c.table_key = t.table_key
inner join [dbo].[dv_satellite_column] sc
on sc.column_key = c.column_key
inner join [dbo].[dv_satellite] sat
on sat.satellite_key = sc.satellite_key
where 1=1
and t.table_key = @source_table_config_key

-- Note that split satellites can only be of 1 type - Link or Hub
select @sat_link_hub_flag = [sat_link_hub_flag] from @satellite_list 


-- Owner Hub Table

if @sat_link_hub_flag = 'H' 
	select   @hub_database			= h.[hub_database]
	        ,@hub_schema			= coalesce([hub_schema], @def_hub_schema, 'dbo')				
			,@hub_table				= h.[hub_name]
			,@hub_surrogate_keyname = [dbo].[fn_GetObjectName] ([dbo].[fn_GetObjectName] ([hub_name], 'hub'),'HubSurrogate')
			,@hub_config_key		= h.[hub_key]
			,@hub_qualified_name	= quotename([hub_database]) + '.' + quotename(coalesce([hub_schema], @def_hub_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_GetObjectName] ([hub_name], 'hub')))	
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
			,@link_surrogate_keyname = [dbo].[fn_GetObjectName] ([dbo].[fn_GetObjectName] ([link_name], 'lnk'),'LnkSurrogate')
			,@link_config_key		= l.[link_key]
			,@link_qualified_name	= quotename([link_database]) + '.' + quotename(coalesce(l.[link_schema], @def_link_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_GetObjectName] ([link_name], 'lnk')))
	from [dbo].[dv_satellite] s
	inner join [dbo].[dv_link] l
	on s.link_key = l.link_key
    where 1=1
    and s.[satellite_key] = (select top 1 sat_config_key from @satellite_list)
 
 -- Get the SQL for the Key Lookup   

EXECUTE [dbo].[dv_load_source_table_key_lookup] @source_system,@source_schema,@source_table, 'N', @temp_table_name OUTPUT, @sql1 OUTPUT

-- Now Get the Satellite Update SQL

set @sql2 = 'DECLARE @version_date_char VARCHAR(20)' + @crlf
set @sql2 = @sql2 + 'DECLARE @version_date datetimeoffset(7)' + @crlf
--set @sql2 = @sql2 + 'select @version_date = max(vault_load_time) from ' + @temp_table_name  + @crlf 
set @sql2 = @sql2 + 'select @version_date = sysdatetimeoffset()'  + @crlf 
set @sql2 = @sql2 + 'select @version_date_char = CONVERT(varchar(50), @version_date) '  + @crlf 
DECLARE c_sat_list CURSOR FOR 
select sat_table
  FROM @satellite_list

OPEN c_sat_list   
FETCH NEXT FROM c_sat_list 
INTO @sat_table		 

WHILE @@FETCH_STATUS = 0   
BEGIN   
EXECUTE [dbo].[dv_load_sat_table] @source_system,@source_schema,@source_table, @sat_table, @temp_table_name, @sql OUTPUT
set @sql2 += @sql
FETCH NEXT FROM c_sat_list 
INTO @sat_table	
END   

CLOSE c_sat_list   
DEALLOCATE c_sat_list
	
set @sql = @sql1 + @sql2

--/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Load The Source into Sat(s)'
IF @_JournalOnOff = 'ON'
	SET @_ProgressText += @SQL
print @SQL
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




