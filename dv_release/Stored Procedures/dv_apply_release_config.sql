
CREATE PROCEDURE [dv_release].[dv_apply_release_config]
(
  @vault_release_number			int			 = NULL
, @DoGenerateError              bit          = 0
, @DoThrowError                 bit			 = 1
)

AS
BEGIN
SET NOCOUNT ON

-- Internal use variables

DECLARE
    @vault_statement		nvarchar(max),
	@vault_change_count		int,
	@release_key			int,
	@release_number			int,
	@change_count			int = 0,
	@currtable				SYSNAME,
	@currschema				SYSNAME,
	@dv_schema_name			sysname,
	@dv_table_name			sysname,
    @parm_definition		nvarchar(500),
	@rc						int	

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
			
	
--set the Parameters for logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @vault_release_number         : ' + COALESCE(cast(@vault_release_number as varchar(20)), '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';

SET @_Step = 'Initialise Variables';

SET @_Step = 'Initialise Release';

select @vault_statement = [release_statement]
from [dv_release].[dv_release_build] 
where [release_number] = @vault_release_number
and [release_statement_type] = 'Header'

/*********************************************************************************************************************/
BEGIN TRANSACTION

set @parm_definition = N'@result int OUTPUT';
exec sp_executesql @vault_statement, @parm_definition, @result=@rc OUTPUT;
if isnull(@rc, 0) <> 1
	RAISERROR('Expected a Single Change to the Release Header. Received %i for Statement: %s', 16, 1, @rc, @vault_statement)

select @release_key = release_key from [dv_release].[dv_release_master] where release_number = @vault_release_number


update [dv_release].[dv_release_master]
set [release_start_datetime] = SYSDATETIMEOFFSET()
where [release_key] = @release_key

-- Extract the Changes per Table
DECLARE dv_table_cursor CURSOR FOR  
SELECT [release_statement], [affected_row_count]
FROM [dv_release].[dv_release_build]
where 1=1
and [release_build_key] = @release_key
and [release_statement_type] = 'Table'
order by [release_statement_sequence]

OPEN dv_table_cursor
FETCH NEXT FROM dv_table_cursor INTO @vault_statement, @change_count

WHILE @@FETCH_STATUS = 0   
BEGIN   
      --print @vault_statement
	  exec sp_executesql @vault_statement, @parm_definition, @result=@rc OUTPUT;
	  if @rc <> @change_count
			RAISERROR('Expected %i changes, Received %i for Statement: %s', 16, 1, @change_count, @rc, @vault_statement)
	  FETCH NEXT FROM dv_table_cursor INTO @vault_statement, @change_count   
END   

CLOSE dv_table_cursor   
DEALLOCATE dv_table_cursor

update [dv_release].[dv_release_master]
	set [release_complete_datetime] = SYSDATETIMEOFFSET(),
        [release_count] = isnull([release_count], 0) + 1
where [release_key] = @release_key
COMMIT

/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Applied Release: ' + cast(@vault_release_number as varchar(20))

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Apply Release: ' + cast(@vault_release_number as varchar(20)) 
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