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
   select 1 / 0
SET @_Step = 'Validate Inputs';

if not exists (select 1 from [dv_scheduler].[dv_run] where [run_key] = @vault_run_key and [run_status] = 'Scheduled')
   raiserror('Run must be "Scheduled" to be able to Start it', 16, 1)

if (SELECT count(*) from [dv_scheduler].[fn_check_manifest_for_circular_reference] (@vault_run_key)) <> 0
	begin
	select @_Message = 'Run Key: ' + cast(@vault_run_key as varchar(20)) + ' Contains Circular References. Please Investigate'
    RAISERROR(@_Message, 16, 1);
	end
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults';
select @delay_in_seconds = cast([dbo].[fn_get_default_value] ('PollDelayInSeconds','Scheduler') as int)
select @delayChar = '00' + format(CONVERT(DATETIME, DATEADD(SECOND, @delay_in_seconds, 0)), ':mm:ss');

set @run_key = @vault_run_key

UPDATE [dv_scheduler].[dv_run] 
	set [run_status] = 'Started'
	   ,[run_start_datetime] = SYSDATETIMEOFFSET()
    where [run_key] = @run_key

SET @_Step = 'Start the Manifest';
while 1=1 -- The loop forcibly exits when all processing has completed
BEGIN
SET @_Step = 'Check Whether the Schedule is Complete'
    if not exists (
		select 1
		from [dv_scheduler].[dv_run] r
		inner join [dv_scheduler].[dv_run_manifest] m
		on m.run_key = r.run_key
		where 1=1
		  and r.run_key = @run_key
		  and isnull(m.run_status, '') <> 'Completed')
		BEGIN
		    UPDATE [dv_scheduler].[dv_run] 
			   set [run_status] = 'Completed'
	              ,[run_end_datetime] = SYSDATETIMEOFFSET()
			   where [run_key] = @run_key
			BREAK
		END
-- has there been a Failure?	
	if exists (
		select 1
		from [dv_scheduler].[dv_run] r
		inner join [dv_scheduler].[dv_run_manifest] m
		on m.run_key = r.run_key
		where 1=1
		and r.run_key = @run_key
		and (isnull(r.run_status, '') = 'Failed' or isnull(m.run_status, '') = 'Failed')
		)
		BEGIN
-- If so, Is there anything to run, assuming that what is queued or running now will succeed?
			if not exists(SELECT 1 FROM [dv_scheduler].[fn_get_waiting_scheduler_tasks] (@run_key, 'Potential'))
			BEGIN
-- If not, Fail the run.
				UPDATE [dv_scheduler].[dv_run] 
					set [run_status] = 'Failed'
	                   ,[run_end_datetime] = SYSDATETIMEOFFSET()
					where [run_key] = @run_key
				BREAK
			END
        END
-- has there been a Cancellation?	
	if exists (
		select 1
		from [dv_scheduler].[dv_run] r
		inner join [dv_scheduler].[dv_run_manifest] m
		on m.run_key = r.run_key
		where 1=1
		and r.run_key = @run_key
		and (isnull(r.run_status, '') = 'Cancelled' or isnull(m.run_status, '') = 'Cancelled')
		)
		BEGIN
-- If so, Is there anything to run, assuming that what is queued or running now will succeed?
			if not exists(SELECT 1 FROM [dv_scheduler].[fn_get_waiting_scheduler_tasks] (@run_key, 'Potential'))
			BEGIN
-- If not, Cancel the run.
				UPDATE [dv_scheduler].[dv_run] 
					set [run_status] = 'Cancelled'
	                   ,[run_end_datetime] = SYSDATETIMEOFFSET()
					where [run_key] = @run_key
				BREAK
			END
        END
-- There is still something to run so Get a list of Tasks to run:
SET @_Step = 'Queue as Set of Tasks'
	DECLARE manifest_cursor CURSOR FOR  
	SELECT [source_unique_name]	
		  ,[source_table_load_type]	
		  ,[queue]
	FROM [dv_scheduler].[fn_get_waiting_scheduler_tasks] (@run_key, DEFAULT)
	order by [priority]
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
	END CONVERSATION @SBDialog
	EXECUTE[dv_scheduler].[dv_manifest_status_update] @run_key ,@source_unique_name ,'Queued'
	COMMIT
	FETCH NEXT FROM manifest_cursor 
	  INTO @source_unique_name
		  ,@source_table_load_type
		  ,@queue
	END   
	
	CLOSE manifest_cursor   
	DEALLOCATE manifest_cursor
    WAITFOR DELAY @delayChar
END
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