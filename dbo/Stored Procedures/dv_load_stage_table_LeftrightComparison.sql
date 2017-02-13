

CREATE PROCEDURE [dbo].[dv_load_stage_table_LeftrightComparison]
(
  @vault_source_version_key		int				= NULL
, @vault_source_load_type		varchar(50)		= NULL
, @vault_runkey					int				= NULL
, @dogenerateerror				bit				= 0
, @dothrowerror					bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly

-- Object Specific Settings
-- Source TableDECLARE @left_object_name nvarchar(128)
declare @left_object_name					nvarchar(128)
       ,@left_object_schema					nvarchar(128)
       ,@left_object_database				nvarchar(128)
       ,@left_object_type					varchar(50)
       ,@left_sat_pit						datetimeoffset(7)
       ,@left_object_filter					nvarchar(4000)
       ,@right_object_name					nvarchar(128)
       ,@right_object_schema				nvarchar(128)
       ,@right_object_database				nvarchar(128)
       ,@right_object_type					varchar(50)
       ,@right_sat_pit						datetimeoffset(7)
       ,@right_object_filter				nvarchar(4000)
       ,@output_database					nvarchar(128)
       ,@output_schema						nvarchar(128)
       ,@output_name						nvarchar(128)
       ,@select_into						bit
       ,@match_key							int
       ,@payload_columns					[dbo].[dv_column_matching_list]
       ,@vault_sql_statement				nvarchar(max)
	   ,@source_load_type					varchar(50)
	   ,@source_unique_name					nvarchar(128)
	   ,@source_pass_load_type_to_proc		bit
	   ,@stage_qualified_name				varchar(512)



DECLARE @crlf char(2) = CHAR(13) + CHAR(10)

DECLARE @SQL1				nvarchar(4000) = ''
DECLARE @ParmDefinition		nvarchar(500);

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


-- set the Parameters for logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @vault_source_version_key     : ' + COALESCE(CAST(@vault_source_version_key AS varchar), 'NULL')
						+ @NEW_LINE + '    @vault_runkey                 : ' + COALESCE(CAST(@vault_runkey AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE
--print @_ProgressText
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF ((@vault_runkey is not null) and ((select count(*) from [dv_scheduler].[dv_run] where @vault_runkey = [run_key] and [run_status]='Started') <> 1))
			RAISERROR('Invalid @vault_runkey provided: %i', 16, 1, @vault_runkey);

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'

-- Object Specific Settings
-- Source Table

select 	 @vault_source_version_key		= source_version_key
		,@source_unique_name			= source_unique_name
        ,@source_load_type				= coalesce(@vault_source_load_type, st.[load_type], 'Full')
		,@source_pass_load_type_to_proc = sv.[pass_load_type_to_proc]
from [dbo].[dv_source_table] st
inner join [dbo].[dv_stage_schema] ss on ss.stage_schema_key = st.stage_schema_key
inner join [dbo].[dv_stage_database] sdb on sdb.stage_database_key = ss.stage_database_key
inner join [dbo].[dv_source_version] sv on sv.[source_table_key] = st.[source_table_key]
where 1=1
and sv.[source_version_key]		= @vault_source_version_key
and sv.[is_current]				= 1

select  @left_object_name		= [left_object_name]
       ,@left_object_schema		= [left_object_schema]
	   ,@left_object_database 	= [left_object_database]
	   ,@left_object_type 		= [left_object_type]
	   ,@left_sat_pit 			= [left_sat_pit]
	   ,@left_object_filter		= [left_object_filter] 
	   ,@right_object_name 		= [right_object_name]
	   ,@right_object_schema 	= [right_object_schema]
	   ,@right_object_database 	= [right_object_database]
	   ,@right_object_type 		= [right_object_type]
	   ,@right_sat_pit 			= [right_sat_pit]
	   ,@right_object_filter 	= [right_object_filter]
	   ,@output_database 		= [output_database]
	   ,@output_schema 			= [output_schema]
	   ,@output_name 			= [output_name]
	   ,@select_into 			= 0
	   ,@match_key 				= [match_key]
from vw_LR_match_config_details 
where [source_version_key] = @vault_source_version_key


if @@ROWCOUNT < 1 RAISERROR('dv_source_table or current dv_source_version missing for source version : %i', 16, 1, @vault_source_version_key);
set @stage_qualified_name = quotename(@output_database) + '.' + quotename(@output_schema) + '.' + quotename(@output_name)
--select * from vw_LR_match_config_details --------------------------------------------------------------------------------------------------------------
--where [source_version_key] = @vault_source_version_key
insert @payload_columns
select [left_column_name], [right_column_name]
from vw_LR_match_config_details 
where [source_version_key] = @vault_source_version_key
if @@ROWCOUNT < 1 RAISERROR('No Column Matches found for Source Version : %i', 16, 1, @vault_source_version_key);

EXECUTE [dv_integrity].[dv_build_match_script] 
   @left_object_name
  ,@left_object_schema
  ,@left_object_database
  ,@left_object_type
  ,@left_sat_pit
  ,@left_object_filter
  ,@right_object_name
  ,@right_object_schema
  ,@right_object_database
  ,@right_object_type
  ,@right_sat_pit
  ,@right_object_filter
  ,@output_database
  ,@output_schema
  ,@output_name
  ,@select_into
  ,@match_key
  ,@payload_columns
  ,@vault_sql_statement OUTPUT

  SET @SQL1 = 'DECLARE @rowcounts TABLE(merge_action nvarchar(10));' + @crlf + 
	        'DECLARE @insertcount int;' + @crlf +
            'DECLARE @version_date datetimeoffset(7)' + @crlf +
			'DECLARE @load_end_datetime datetimeoffset(7)' + @crlf +
			'SELECT @version_date = sysdatetimeoffset()'  + @crlf +
			'BEGIN TRANSACTION' + @crlf +
			@vault_sql_statement + @crlf +
			'SELECT @insertcount = @@ROWCOUNT;' + @crlf +
			'SELECT @load_end_datetime = sysdatetimeoffset();' + @crlf 
-- Log Completion

set @SQL1 += 'EXECUTE [dv_log].[dv_log_progress] ''stg'',''' + @output_name + ''',''' + @output_schema + ''',''' +  @output_database + ''',' 
set @SQL1 += '''' + @source_unique_name + ''',@@SPID,' + isnull(cast(@vault_runkey as varchar), 'NULL') + ', @version_date, null, @version_date, @load_end_datetime, @insertcount, 0, 0, 0' + @crlf
set @SQL1 += 'COMMIT;' + @crlf
/*--------------------------------------------------------------------------------------------------------------*/

SET @_Step = 'Load The Stage Table'
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @SQL1 + @crlf
--print @SQL1
EXECUTE sp_executesql @SQL1;

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Loaded Object: ' + @stage_qualified_name 

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Load Object: ' + @stage_qualified_name
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
--print @_ProgressText
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