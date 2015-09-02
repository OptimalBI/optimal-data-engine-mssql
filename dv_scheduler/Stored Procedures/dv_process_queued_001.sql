
CREATE PROCEDURE [dv_scheduler].[dv_process_queued_001]

WITH EXECUTE AS OWNER
AS
BEGIN
SET NOCOUNT ON
-- Declare Variables for use by the SP.
DECLARE @task						nvarchar(512)
       ,@message_type_name			nvarchar(512)
	   ,@queue_name					nvarchar(512)
	   ,@msg						xml
	   ,@msgChar					nvarchar(200)
	   ,@dialog_handle				uniqueidentifier
	   ,@vault_source_system_name	nvarchar(50)
	   ,@vault_source_table_schema	nvarchar(128)
	   ,@vault_source_table_name	nvarchar(128)

-- Set Constant Values

set @queue_name = 'dv_scheduler_m001' -- Change for each Reveiver Procedure

-- Log4TSQL Journal Constants 
DECLARE @dogenerateerror		bit		 = 0
	   ,@dothrowerror			bit		 = 1

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
						+ @NEW_LINE + '    @queue_name                   : ' + COALESCE(@queue_name, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

SET @dialog_handle = NULL
SET @_Step = 'Pull From the Queue';
WAITFOR ( RECEIVE TOP (1) @dialog_handle		= conversation_handle
                        , @message_type_name	= message_type_name
						, @msg					= convert(xml,message_body)
FROM [dv_scheduler_q001])   -- Change for each Receiver Procedure
                        , TIMEOUT 1000
IF (@@ROWCOUNT > 0)
	BEGIN
	SET @_Step = 'Process the Message';	
	SET @msgChar = cast(@msg as varchar(200))
	IF @message_type_name = @queue_name
		BEGIN
		    SELECT
				 @vault_source_system_name	= x.value('(/Request/SourceSystem)[1]','VARCHAR(50)')
				,@vault_source_table_schema	= x.value('(/Request/SourceSchema)[1]','VARCHAR(128)')
				,@vault_source_table_name	= x.value('(/Request/SourceTable)[1]','VARCHAR(128)')
			FROM @msg.nodes('/Request') AS T(x);
			WAITFOR DELAY '00:00:10'
			exec [dbo].[dv_load_source_table] @vault_source_system_name, @vault_source_table_schema, @vault_source_table_name
			--END CONVERSATION @dialog_handle;
		END		
	ELSE 
		BEGIN		
			set @_Message = 'Message ' + quotename(@message_type_name) +  'Received but not Processed on Queue: ' + quotename(@queue_name) + quotename(@msgChar)
		    set @_ProgressText = @_ProgressText + @_Message + @NEW_LINE
			--END CONVERSATION @dialog_handle;
		END
	END
SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Completed Load of: ' + quotename(@vault_source_system_name) + '.' + quotename(@vault_source_table_schema) + '.' + quotename(@vault_source_table_name)
print @_Message
END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed Load of: ' + quotename(@vault_source_system_name) + '.' + quotename(@vault_source_table_schema) + '.' + quotename(@vault_source_table_name)
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