CREATE PROCEDURE [dv_scheduler].[dv_populate_manifest]
(
	 @schedule_name		varchar(4000)
	,@run_key			int
	,@DoGenerateError   bit          = 0
	,@DoThrowError      bit			 = 1
)
AS
BEGIN
SET NOCOUNT ON;

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
						+ @NEW_LINE + '    @schedule_name	: ' + COALESCE(@schedule_name					, '<NULL>')
						+ @NEW_LINE + '    @run_key			: ' + COALESCE(CAST(@run_key as varchar(20))	, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError    : ' + COALESCE(CAST(@DoThrowError AS varchar)	, '<NULL>')
						+ @NEW_LINE
BEGIN TRANSACTION
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';

SET @_Step = 'Initialise Variables';

SET @_Step = 'Initialise Release';


-- insert all tables to be run into the dv_run_manifest table
INSERT INTO dv_scheduler.dv_run_manifest (
	 [run_key]
	,[source_unique_name]
	,[source_table_key]
	,[source_table_load_type]
	,[priority]
	,[queue]
	)
SELECT @run_key AS run_key
	,[src_table].[source_unique_name]
	,[src_table].[source_table_key]
	,case when [schd_src_table].[source_table_load_type] = 'Default' then [src_table].[load_type] else [schd_src_table].[source_table_load_type] end
	,[schd_src_table].[priority]
	,[schd_src_table].[queue]
FROM dv_scheduler.vw_dv_schedule_current AS schd
INNER JOIN dv_scheduler.vw_dv_schedule_source_table_current AS schd_src_table 
	ON schd.schedule_key = schd_src_table.schedule_key
INNER JOIN dbo.dv_source_table AS src_table 
	ON schd_src_table.source_table_key = src_table.[source_table_key]
--INNER JOIN dbo.dv_source_system AS src_system 
--	ON src_table.system_key = src_system.[source_system_key]
WHERE upper(schedule_name) IN (
		SELECT replace(Item, ' ', '')
		FROM dbo.fn_split_strings(upper(@schedule_name), ',')
		)
--and (schd_src_table.is_deleted | schd.is_deleted <> 1);  -- Bitwise OR to check if one or both bits are set


/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Populated Manifest for Schedule: ' + @schedule_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Populate Manifest for Schedule: ' + @schedule_name
IF (XACT_STATE() = -1) -- uncommitable transaction
OR (@@TRANCOUNT > 0) -- AND XACT_STATE() != 1) -- undocumented uncommitable transaction
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