
CREATE procedure [dv_integrity].[dv_hub_metrics]
(
   @hub_key						int				= 0
  ,@stage_database				varchar(128)	= 'ODV_Metrics_Stage'
  ,@stage_schema				varchar(128)	= 'Stage'
  ,@stage_table					varchar(128)	= 'Integrity_Hub_Counts'
  ,@dogenerateerror				bit				= 0
  ,@dothrowerror				bit				= 1
)
as
begin
set nocount on
-- Local Defaults Values
declare @crlf char(2) = char(13) + char(10)
-- Global Defaults
DECLARE  
		 @def_global_default_load_date_time	varchar(128)
-- hub Table
declare  @hub_qualified_name				varchar(512)
        ,@hub_data_source_col               varchar(50)
		,@hub_load_date_time				varchar(50)
		
-- Stage Table
declare  @stage_qualified_name				varchar(512)
--  Working Storage
declare @SQL								nvarchar(max) = ''
declare @hub_loop_key						bigint
declare @hub_loop_stop_key					bigint
declare @run_time							varchar(50)

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
SET @_JournalOnOff      = log4.GetJournalControl(@_FunctionName, 'IntegrityChecks');  -- left Group Name as HOWTO for now.
select @_FunctionName   = isnull(OBJECT_NAME(@@PROCID), 'Test');

-- set Log4TSQL Parameters for Logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @hub_key                     : ' + COALESCE(CAST(@hub_key AS varchar), 'NULL') 
						+ @NEW_LINE + '    @stage_database               : ' + @stage_database					
						+ @NEW_LINE + '    @stage_schema                 : ' + @stage_schema				
						+ @NEW_LINE + '    @stage_table                  : ' + @stage_table					    
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'
select
-- Global Defaults
	@def_global_default_load_date_time	= cast([dbo].[fn_get_default_value] ('DefaultLoadDateTime','Global')	as varchar(128))

-- hub Defaults
select @hub_data_source_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'hub'
and object_column_type = 'Data_Source'

select @hub_load_date_time = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'hub'
and object_column_type = 'Load_Date_Time'

 --Stage Values
set @stage_qualified_name = quotename(@stage_database) + '.' + quotename(@stage_schema) + '.' + quotename(@stage_table) 
select @run_time = cast(sysdatetimeoffset() as varchar(50))

-- Truncate the Stage Table
set @SQL = 'truncate table ' + @stage_qualified_name
exec(@SQL)

set @_Step = 'Build the test SQL'
select @hub_loop_key = case when isnull(@hub_key, 0) = 0 then max(hub_key) else @hub_key end from [dbo].[dv_hub]
set @hub_loop_stop_key = isnull(@hub_key, 0)
while @hub_loop_key >= @hub_loop_stop_key
/**********************************************************************************************************************/
begin
if @hub_loop_key > 0
begin
	select @SQL =
	'if exists (select 1 from ' + quotename(l.[hub_database]) + '.[information_schema].[tables] where [table_schema] = ''' + l.[hub_schema] + ''' and [table_name] = ''' + [dbo].[fn_get_object_name] (l.hub_name, 'hub') + ''')' + @crlf +
	'begin' + @crlf +
	'insert ' + @stage_qualified_name + @crlf +
	'select ''' + @run_time + '''' + @crlf +
		+',' + cast(l.hub_key as varchar(50)) + ' as [object_key]' + @crlf
		+ ',''' + l.hub_name + ''' as [object_name]' + @crlf
		+ ',l.' + @hub_data_source_col + ' as [record_source]' + @crlf
		+ ',ss.[source_system_name]' + @crlf
		+ ',cfg.[source_table_name]' + @crlf

	+ ',count_big(*) as [RowCount]' + @crlf
	+'from ' + quotename(l.[hub_database]) + '.' + quotename(l.[hub_schema]) + '.' + quotename([dbo].[fn_get_object_name] (l.hub_name, 'hub')) +' l' + @crlf
	+'left join [dbo].[dv_source_table] cfg on cfg.source_table_key = l.' + @hub_data_source_col + @crlf
	+'left join [dbo].[dv_source_system] ss on ss.[source_system_key] = cfg.[system_key]' + @crlf
	+'where ' + @hub_load_date_time + ' <= ''' + @run_time + '''' + @crlf + 
	+ 'group by l.' + @hub_data_source_col + ', ss.[source_system_name],cfg.[source_table_name]' + @crlf 
	+ 'end' + @crlf
	+ @crlf + @crlf
	from [dbo].[dv_hub] l
	where hub_key = @hub_loop_key
	--select @SQL
	execute sp_executesql @SQL
end
select @hub_loop_key = max(hub_key) from [dbo].[dv_hub]
		where hub_key < @hub_loop_key
end
/**********************************************************************************************************************/

SET @_Step = 'Extract the Stats'
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @SQL + @crlf

set @_Step = 'Completed'

/**********************************************************************************************************************/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Ran Hub Integrity Checker' 

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Run Hub Integrity Checker' + @hub_qualified_name
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