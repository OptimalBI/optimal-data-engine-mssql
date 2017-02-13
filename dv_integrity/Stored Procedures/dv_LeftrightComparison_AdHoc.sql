


CREATE PROCEDURE [dv_integrity].[dv_LeftrightComparison_AdHoc]
(
  @left_object_name					nvarchar(128)
, @left_object_schema				nvarchar(128)
, @left_object_database				nvarchar(128)
, @left_object_type					varchar(50)
, @left_sat_pit						datetimeoffset(7)
--, @left_object_filter				nvarchar(4000)
, @right_object_name				nvarchar(128)
, @right_object_schema				nvarchar(128)
, @right_object_database			nvarchar(128)
, @right_object_type				varchar(50)
, @right_sat_pit					datetimeoffset(7)
--, @right_object_filter				nvarchar(4000)
, @output_database					nvarchar(128)
, @output_schema					nvarchar(128)
, @output_name						nvarchar(128)
, @select_into						bit
, @match_key						int
, @payload_columns					[dbo].[dv_column_matching_list] READONLY
, @dogenerateerror					bit				= 0
, @dothrowerror						bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly

-- Object Specific Settings
-- Source TableDECLARE @left_object_name nvarchar(128)
declare @vault_sql_statement				nvarchar(max)
	   ,@source_load_type					varchar(50)
	   ,@source_unique_name					nvarchar(128)
	   ,@source_pass_load_type_to_proc		bit
	   ,@stage_qualified_name				varchar(512)
	   ,@payload_columns_string				nvarchar(max)
	   ,@left_object_filter				    nvarchar(4000)
	   ,@right_object_filter				nvarchar(4000)
	   --,@left_sat_pit						datetimeoffset(7)
	   --,@right_sat_pit						datetimeoffset(7)

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

select @payload_columns_string =''
select @payload_columns_string +=  'Left: ' + [left_column_name] + ', Right: ' + [right_column_name] + @crlf
      from @payload_columns
-- set the Parameters for logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @left_object_name				: ' + COALESCE(@left_object_name, '<NULL>')
						+ @NEW_LINE + '    @left_object_schema				: ' + COALESCE(@left_object_schema, '<NULL>')
						+ @NEW_LINE + '    @left_object_database			: ' + COALESCE(@left_object_database, '<NULL>')
						+ @NEW_LINE + '    @left_object_type				: ' + COALESCE(@left_object_type, '<NULL>')
						+ @NEW_LINE + '    @left_sat_pit					: ' + COALESCE(CAST(@left_sat_pit AS VARCHAR(50)), '<NULL>') --
						--+ @NEW_LINE + '    @left_object_filter				: ' + COALESCE(@left_object_filter, '<NULL>')
						+ @NEW_LINE + '    @right_object_name				: ' + COALESCE(@right_object_name, '<NULL>')
						+ @NEW_LINE + '    @right_object_schema				: ' + COALESCE(@right_object_schema, '<NULL>')
						+ @NEW_LINE + '    @right_object_database			: ' + COALESCE(@right_object_database, '<NULL>')
						+ @NEW_LINE + '    @right_object_type				: ' + COALESCE(@right_object_type, '<NULL>')
						+ @NEW_LINE + '    @right_sat_pit					: ' + COALESCE(CAST(@right_sat_pit AS VARCHAR(50)), '<NULL>')
						--+ @NEW_LINE + '    @right_object_filter				: ' + COALESCE(@object_name, '<NULL>')
						+ @NEW_LINE + '    @output_database					: ' + COALESCE(@output_database, '<NULL>')
						+ @NEW_LINE + '    @output_schema					: ' + COALESCE(@output_schema, '<NULL>')
						+ @NEW_LINE + '    @output_name						: ' + COALESCE(@output_name, '<NULL>')
						+ @NEW_LINE + '    @select_into						: ' + COALESCE(CAST(@select_into AS VARCHAR(5)), '<NULL>')
						+ @NEW_LINE + '    @match_key						: ' + COALESCE(CAST(@match_key AS VARCHAR(5)), '<NULL>')
						+ @NEW_LINE + '    @payload_columns_string			: ' + COALESCE(@payload_columns_string, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError                 : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                    : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE
--print @_ProgressText
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';


/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'

-- Object Specific Settings

SET @_Step = 'Build The Script'
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

  SET @SQL1 = 'BEGIN TRANSACTION' + @crlf +
			  @vault_sql_statement + @crlf +
		      'COMMIT;' + @crlf
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Execute The Script'

IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @SQL1 + @crlf
print @SQL1
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