CREATE procedure [dv_scheduler].[dv_process_manifest]
( 
  @vault_run_key int
, @dogenerateerror               bit            = 0
, @dothrowerror                  bit			= 1
)
as

BEGIN
set nocount on

DECLARE @msg						XML
       ,@SBDialog					uniqueidentifier
	   ,@source_unique_name		    varchar(128)
	   ,@source_table_load_type		varchar(50)
	   ,@queue						varchar(10)
	   ,@run_key					int
	   ,@delay_in_seconds			int
	   ,@delayChar					char(8)
	   ,@run_status					varchar(50)


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
						+ @NEW_LINE + '    @vault_run_key                : ' + COALESCE(CAST(@vault_run_key AS varchar(20)), '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   SELECT 1 / 0
SET @_Step = 'Validate Inputs';

IF NOT EXISTS (select 1 from [dv_scheduler].[dv_run] WHERE [run_key] = @vault_run_key and [run_status] = 'Scheduled')
   RAISERROR('Run must be "Scheduled" to be able to Start it', 16, 1)

if (SELECT COUNT(*) FROM [dv_scheduler].[fn_check_manifest_for_circular_reference] (@vault_run_key)) <> 0
	BEGIN
	SELECT @_Message = 'Run Key: ' + CAST(@vault_run_key AS VARCHAR(20)) + ' Contains Circular References. Please Investigate'
    RAISERROR(@_Message, 16, 1);
	END
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults';
SELECT @delay_in_seconds = CAST([dbo].[fn_get_default_value] ('PollDelayInSeconds','Scheduler') as int)
SELECT @delayChar = '00' + FORMAT(CONVERT(DATETIME, DATEADD(SECOND, @delay_in_seconds, 0)), ':mm:ss');
SET @run_key = @vault_run_key


--****************************************************************************************************************
SET @_Step = 'Start the Manifest';
--****************************************************************************************************************
UPDATE [dv_scheduler].[dv_run] 
	SET [run_status] = 'Started'
	   ,[run_start_datetime] = SYSDATETIMEOFFSET()
    WHERE [run_key] = @run_key

--****************************************************************************************************************
SET @_Step = 'Loop until the Manifest completes processing';
--****************************************************************************************************************
WHILE 1=1 
BEGIN
--> If the run has been cancelled, "Cancel" all waiting tasks to save them from beig Queued and then Cancelled anyway.
SET @_Step = 'Check whether the Schedule has been Cancelled'
	UPDATE m
	SET [run_status] = 'Cancelled'
	FROM [dv_scheduler].[dv_run] r
	INNER JOIN [dv_scheduler].[dv_run_manifest] m
	ON m.[run_key] = r.[run_key]
	WHERE 1=1
		AND r.[run_key] = @run_key
		AND ISNULL(r.[run_status], '') = 'Cancelled'
		AND ISNULL(m.[run_status], '') = 'Scheduled'

--****************************************************************************************************************
SET @_Step = 'Check Whether the Schedule is Complete'
--****************************************************************************************************************
--> To exit the loop:
	--> Nothing must be waiting to finish ("Queued" or "Processing"). 
	--> Also, there may be no further tasks, which could be placed on the Queue.
    IF NOT EXISTS (
		SELECT 1 FROM [dv_scheduler].[dv_run_manifest] 
		WHERE ISNULL([run_status], '') IN ('Queued', 'Processing')
		  AND [run_key] = @run_key
		  )
		IF NOT EXISTS (SELECT 1 FROM [dv_scheduler].[fn_get_waiting_scheduler_tasks] (@run_key, DEFAULT))
			BREAK   

--****************************************************************************************************************
SET @_Step = 'Queue eligible Tasks'
--****************************************************************************************************************
--> There is still something running or to be run, so check for anything eligble to be Queued:

	DECLARE manifest_cursor CURSOR FOR  
	SELECT [source_unique_name]	
		  ,[source_table_load_type]	
		  ,[queue]
	FROM [dv_scheduler].[fn_get_waiting_scheduler_tasks] (@run_key, DEFAULT)
	ORDER BY [priority]
	OPEN manifest_cursor
	FETCH NEXT FROM manifest_cursor 
	  INTO @source_unique_name
		  ,@source_table_load_type
		  ,@queue
	
	WHILE @@FETCH_STATUS = 0   
	BEGIN   
		
	SET @msg = N'
	<Request>
	      <RunKey>'				+ isnull(cast(@run_key as varchar(20)), '')		+ N'</RunKey>
		  <SourceUniqueName>'	+ isnull(@source_unique_name, '')				+ N'</SourceUniqueName>
		  <RunType>'			+ isnull(@source_table_load_type, '')			+ N'</RunType>
	</Request>'
	SET @_Step = 'Queue a Single Task: ' + cast(@msg as varchar(4000))
	BEGIN TRANSACTION
	IF @queue = '001'
	BEGIN
		BEGIN DIALOG CONVERSATION @SBDialog
			FROM SERVICE dv_scheduler_s001
			TO SERVICE  'dv_scheduler_s001'
			ON CONTRACT  dv_scheduler_c001
			WITH ENCRYPTION = OFF;
			--Send messages on Dialog
		SEND ON CONVERSATION @SBDialog
			MESSAGE TYPE dv_scheduler_m001 (@Msg)
	END
	ELSE
	IF @queue = '002'
	BEGIN
		BEGIN DIALOG CONVERSATION @SBDialog
			FROM SERVICE dv_scheduler_s002
			TO SERVICE	'dv_scheduler_s002'
			ON CONTRACT	 dv_scheduler_c002
			WITH ENCRYPTION = OFF;
			--Send messages on Dialog
		SEND ON CONVERSATION @SBDialog
			MESSAGE TYPE dv_scheduler_m002 (@Msg)
	END
	ELSE
	BEGIN
		BEGIN DIALOG CONVERSATION @SBDialog
			FROM SERVICE dv_scheduler_sAgent001
			TO SERVICE	'dv_scheduler_sAgent001'
			ON CONTRACT	 dv_scheduler_cAgent001
			WITH ENCRYPTION = OFF;
			--Send messages on Dialog
		SEND ON CONVERSATION @SBDialog
			MESSAGE TYPE dv_scheduler_mAgent001 (@Msg)
	END
	END CONVERSATION @SBDialog
	EXECUTE [dv_scheduler].[dv_manifest_status_update] @run_key ,@source_unique_name ,'Queued'
	COMMIT
	FETCH NEXT FROM manifest_cursor 
	  INTO @source_unique_name
		  ,@source_table_load_type
		  ,@queue
	END   
	
	CLOSE manifest_cursor   
	DEALLOCATE manifest_cursor
--****************************************************************************************************************
SET @_Step = 'Wait before Checking for eligible Tasks again'
--****************************************************************************************************************
    WAITFOR DELAY @delayChar
END

--****************************************************************************************************************
SET @_Step = 'Processing is Complete. Set the Run Status.'
--****************************************************************************************************************
SELECT @run_status = ISNULL([run_status], '') 
	FROM [dv_scheduler].[dv_run] r
	WHERE [run_key] = @run_key 
-- If the schedule has been cancelled, leave the status as is
IF @run_status <> 'Cancelled'
-- Has there been a Failure?	
	IF EXISTS (
		SELECT 1
		FROM [dv_scheduler].[dv_run] r
		INNER JOIN [dv_scheduler].[dv_run_manifest] m
		ON m.[run_key] = r.[run_key]
		WHERE 1=1
		AND r.[run_key] = @run_key
		AND (ISNULL(m.[run_status], '') = 'Failed')
		)
		SET @run_status = 'Failed'
-- Has there been a Cancelled Task?
	ELSE IF EXISTS (
		SELECT 1
		FROM [dv_scheduler].[dv_run] r
		INNER JOIN [dv_scheduler].[dv_run_manifest] m
		ON m.[run_key] = r.[run_key]
		WHERE 1=1
		AND r.[run_key] = @run_key
		AND (ISNULL(m.[run_status], '') = 'Cancelled')
		)
		SET @run_status = 'Cancelled'
-- In case of a random Status - should never happen
	ELSE IF EXISTS (
		SELECT 1
		FROM [dv_scheduler].[dv_run] r
		INNER JOIN [dv_scheduler].[dv_run_manifest] m
		ON m.[run_key] = r.[run_key]
		WHERE 1=1
		AND r.[run_key] = @run_key
		AND (ISNULL(m.[run_status], '') <> 'Completed')
		)
		SET @run_status = 'Unknown'
	ELSE
-- Otherwise, set to Completed.
	    SET @run_status = 'Completed'

	UPDATE [dv_scheduler].[dv_run] 
	SET [run_status] = @run_status
	   ,[run_end_datetime] = SYSDATETIMEOFFSET()
	WHERE [run_key] = @run_key
-- If the final Status isn't Completed, Raise an error:
	IF @run_status <> 'Completed'
		RAISERROR('Scheduler Run: %i Completed unsuccessfully with Status: %s', 16, 1, @run_key, @run_status);

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Completed Schedule with Run_Key: ' + cast(@run_key as varchar(20))

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Complete Schedule with Run_Key: ' + cast(@run_key as varchar(20))
IF (XACT_STATE() = -1) OR (@@TRANCOUNT > 0) -- undocumented uncommitable transaction
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
