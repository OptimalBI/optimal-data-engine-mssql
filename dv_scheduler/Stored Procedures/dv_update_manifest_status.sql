
CREATE PROCEDURE [dv_scheduler].[dv_update_manifest_status]
(
  @vault_run_key				 int			= NULL
, @vault_source_system_name      varchar(50)	= NULL
, @vault_source_table_schema     varchar(128)   = NULL
, @vault_source_table_name       varchar(128)   = NULL
, @vault_run_status				 varchar(20)	= NULL
, @dogenerateerror               bit            = 0
, @dothrowerror                  bit			= 1
)
AS
BEGIN
SET NOCOUNT ON
-- Log4TSQL Journal Constants 
--DECLARE @SEVERITY_CRITICAL      smallint = 1;
--DECLARE @SEVERITY_SEVERE        smallint = 2;
--DECLARE @SEVERITY_MAJOR         smallint = 4;
--DECLARE @SEVERITY_MODERATE      smallint = 8;
--DECLARE @SEVERITY_MINOR         smallint = 16;
--DECLARE @SEVERITY_CONCURRENCY   smallint = 32;
--DECLARE @SEVERITY_INFORMATION   smallint = 256;
--DECLARE @SEVERITY_SUCCESS       smallint = 512;
--DECLARE @SEVERITY_DEBUG         smallint = 1024;
--DECLARE @NEW_LINE               char(1)  = CHAR(10);

---- Log4TSQL Standard/ExceptionHandler variables
--DECLARE	  @_Error         int
--		, @_RowCount      int
--		, @_Step          varchar(128)
--		, @_Message       nvarchar(512)
--		, @_ErrorContext  nvarchar(512)

---- Log4TSQL JournalWriter variables
--DECLARE   @_FunctionName			varchar(255)
--		, @_SprocStartTime			datetime
--		, @_JournalOnOff			varchar(3)
--		, @_Severity				smallint
--		, @_ExceptionId				int
--		, @_StepStartTime			datetime
--		, @_ProgressText			nvarchar(max)

--SET @_Error             = 0;
--SET @_FunctionName      = OBJECT_NAME(@@PROCID);
--SET @_Severity          = @SEVERITY_INFORMATION;
--SET @_SprocStartTime    = sysdatetimeoffset();
--SET @_ProgressText      = '' 
--SET @_JournalOnOff      = log4.GetJournalControl(@_FunctionName, 'HOWTO');  -- left Group Name as HOWTO for now.


---- set the Parameters for logging:

--SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
--						+ @NEW_LINE + '    @vault_run_key                : ' + COALESCE(CAST(@vault_run_key AS varchar(20)), '<NULL>')
--						+ @NEW_LINE + '    @vault_source_system_name     : ' + COALESCE(@vault_source_system_name, '<NULL>')
--						+ @NEW_LINE + '    @vault_source_table_schema    : ' + COALESCE(@vault_source_table_schema, '<NULL>')
--						+ @NEW_LINE + '    @vault_source_table_name      : ' + COALESCE(@vault_source_table_name, '<NULL>')
--						+ @NEW_LINE + '    @vault_run_status			 : ' + COALESCE(@vault_run_status, '<NULL>')
--						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
--						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
--						+ @NEW_LINE

--BEGIN TRY
--SET @_Step = 'Generate any required error';
--IF @DoGenerateError = 1
--   select 1 / 0
--SET @_Step = 'Validate Inputs';

/*--------------------------------------------------------------------------------------------------------------*/
--SET @_Step = 'Get Defaults';
if @vault_run_status in ('Queued', 'Failed')
	UPDATE [dv_scheduler].[dv_run_manifest]
			SET   [run_status] = @vault_run_status
			WHERE [run_key] = @vault_run_key
			  AND [source_system_name]	= @vault_source_system_name
			  AND [source_table_schema] = @vault_source_table_schema
              AND [source_table_name]	= @vault_source_table_name
else if @vault_run_status in ('Processing')
	UPDATE [dv_scheduler].[dv_run_manifest]
			SET [run_status] = @vault_run_status
			   ,[start_datetime] = SYSDATETIMEOFFSET()
               ,[session_id] = @@SPID
			WHERE [run_key] = @vault_run_key
			  AND [source_system_name] = @vault_source_system_name
			  AND [source_table_schema] = @vault_source_table_schema
              AND [source_table_name] = @vault_source_table_name
else if @vault_run_status in ('Completed')
    UPDATE [dv_scheduler].[dv_run_manifest]
			SET [completed_datetime] = SYSDATETIMEOFFSET()
               ,[run_status] = 'Completed'
			WHERE [run_key] = @vault_run_key
			  AND [source_system_name] = @vault_source_system_name
			  AND [source_table_schema] = @vault_source_table_schema
              AND [source_table_name] = @vault_source_table_name
else
    raiserror('@vault_run_status must be one of: (Processing, Queued, Failed, Completed)', 16, 1)


/*--------------------------------------------------------------------------------------------------------------*/

--SET @_ProgressText  = @_ProgressText + @NEW_LINE
--				+ 'Step: [' + @_Step + '] completed ' 

--IF @@TRANCOUNT > 0 COMMIT TRAN;

--SET @_Message   = 'Successfully Completed Schedule Status Update with Run_Key: ' + cast(@vault_run_key as varchar(20))

--END TRY
--BEGIN CATCH
--SET @_ErrorContext	= 'Failed to Complete Schedule Status Update with Run_Key: ' + cast(@vault_run_key as varchar(20))
--IF (XACT_STATE() = -1) -- uncommitable transaction
--OR (@@TRANCOUNT > 0 AND XACT_STATE() != 1) -- undocumented uncommitable transaction
--	BEGIN
--		ROLLBACK TRAN;
--		SET @_ErrorContext = @_ErrorContext + ' (Forced rolled back of all changes)';
--	END
	
--EXEC log4.ExceptionHandler
--		  @ErrorContext  = @_ErrorContext
--		, @ErrorNumber   = @_Error OUT
--		, @ReturnMessage = @_Message OUT
--		, @ExceptionId   = @_ExceptionId OUT
--;
--END CATCH

----/////////////////////////////////////////////////////////////////////////////////////////////////
--OnComplete:
----/////////////////////////////////////////////////////////////////////////////////////////////////

--	--! Clean up

--	--!
--	--! Use dbo.udf_FormatElapsedTime() to get a nicely formatted run time string e.g.
--	--! "0 hr(s) 1 min(s) and 22 sec(s)" or "1345 milliseconds"
--	--!
--	IF @_Error = 0
--		BEGIN
--			SET @_Step			= 'OnComplete'
--			SET @_Severity		= @SEVERITY_SUCCESS
--			SET @_Message		= COALESCE(@_Message, @_Step)
--								+ ' in a total run time of ' + log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
--			SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
--		END
--	ELSE
--		BEGIN
--			SET @_Step			= COALESCE(@_Step, 'OnError')
--			SET @_Severity		= @SEVERITY_SEVERE
--			SET @_Message		= COALESCE(@_Message, @_Step)
--								+ ' after a total run time of ' + log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
--			SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
--		END

--	IF @_JournalOnOff = 'ON'
--		EXEC log4.JournalWriter
--				  @Task				= @_FunctionName
--				, @FunctionName		= @_FunctionName
--				, @StepInFunction	= @_Step
--				, @MessageText		= @_Message
--				, @Severity			= @_Severity
--				, @ExceptionId		= @_ExceptionId
--				--! Supply all the progress info after we've gone to such trouble to collect it
--				, @ExtraInfo        = @_ProgressText

--	--! Finally, throw an exception that will be detected by the caller
--	IF @DoThrowError = 1 AND @_Error > 0
--		RAISERROR(@_Message, 16, 99);

--	SET NOCOUNT OFF;

--	--! Return the value of @@ERROR (which will be zero on success)
--	RETURN (@_Error);
END