
--exec [dbo].[dv_load_stage_table_SSISPackage] 2

CREATE PROCEDURE [dbo].[dv_load_stage_table_SSISPackage]
  @vault_source_version_key		int				= NULL
, @vault_source_load_type		varchar(50)		= NULL
, @vault_runkey					int				= NULL
, @dogenerateerror				bit				= 0
, @dothrowerror					bit				= 1


AS
BEGIN
SET NOCOUNT ON


-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly

-- Object Specific Settings
-- Source Table
declare  @stage_qualified_name				nvarchar(512)
		,@source_load_type					nvarchar(512)
		,@source_type						varchar(50)
		,@stage_package_name				nvarchar(260)
		,@stage_pass_load_type_to_proc		bit
		,@error_message						varchar(256)
		,@ssis_project_name					nvarchar(128)
        ,@ssis_folder_name					nvarchar(128)
		,@ssis_source_password				nvarchar(128)
		,@execution_id						bigint 
		,@ssis_return						int				

DECLARE @crlf char(2) = CHAR(13) + CHAR(10)

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
						+ @NEW_LINE + '    @vault_source_load_type       : ' + COALESCE(@vault_source_load_type, '<NULL>')
						+ @NEW_LINE + '    @vault_runkey                 : ' + COALESCE(CAST(@vault_runkey AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF ((@vault_runkey is not null) and ((select count(*) from [dv_scheduler].[dv_run] where @vault_runkey = [run_key]) <> 1))
			RAISERROR('Invalid @vault_runkey provided: %i', 16, 1, @vault_runkey);

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'

-- Object Specific Settings
-- Source Table

select 	 @stage_qualified_name			= quotename(sdb.[stage_database_name]) + '.' + quotename(ssc.[stage_schema_name]) + '.' + quotename(st.[stage_table_name])
		,@source_load_type				= @vault_source_load_type
		,@source_type					= sv.[source_type]
		,@stage_package_name			= sv.[source_procedure_name] + '.dtsx'
		,@stage_pass_load_type_to_proc  = sv.[pass_load_type_to_proc]
		,@ssis_folder_name				= ss.package_folder
		,@ssis_project_name				= ss.package_project
		,@ssis_source_password			= conn.connection_password
from [dbo].[dv_source_system] ss
inner join [dbo].[dv_source_table] st on st.[system_key] = ss.[source_system_key]
inner join [dbo].[dv_connection] conn on conn.connection_name = ss.project_connection_name
inner join [dbo].[dv_stage_schema] ssc on ssc.stage_schema_key = st.stage_schema_key
inner join [dbo].[dv_stage_database] sdb on sdb.stage_database_key = ssc.stage_database_key
inner join [dbo].[dv_source_version] sv on sv.[source_table_key] = st.[source_table_key]
where 1=1
and sv.[source_version_key]		= @vault_source_version_key
and sv.[is_current]				= 1
if @@ROWCOUNT <> 1 RAISERROR('dv_source_table or current dv_source_version missing for source version : %i', 16, 1, @vault_source_version_key);
if @source_type <> 'SSISPackage' RAISERROR('invalid source_type provided : %s', 16, 1, @source_type);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Executing Package: '+ quotename(@ssis_project_name) + '.' + quotename(@ssis_folder_name) + '.' + quotename(@stage_package_name);


Set @_Step = 'Create Execution'

EXEC [SSISDB].[catalog].[create_execution] 
      @folder_name		= @ssis_folder_name
	, @project_name		= @ssis_project_name
	, @package_name		= @stage_package_name
	, @reference_id		= NULL
	, @execution_id		= @execution_id OUTPUT


/****************************************************************************************
Note that SSIS Catalog is inclined to throw deadlocs when adding Parameters to and execution in parallel.
For this reason, each parameter add is enclosed in it's own try/catch block and loop.
This allows for a number of retries before it gives in and fails.  */

SELECT @ssis_return = cast([dbo].[fn_get_default_value] ('RetrySSISParameters','Scheduler') as int)

/****************************************************************************************/
-- System Parameters
Set @_Step = 'Set Execution to Synchronised'
Lab510:
BEGIN TRY
EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type		= 50				
	, @parameter_name	= N'SYNCHRONIZED'	-- Proc will wait for the Package to complete - Do Not Remove!
	, @parameter_value	= 1
END TRY
BEGIN CATCH
if @ssis_return < 1 RAISERROR('The package %s : %s : %s failed, unable to create a parameter in the catalog', 16, 1, @ssis_folder_name, @ssis_project_name, @stage_package_name)
set @ssis_return -=1
goto Lab510
END CATCH
-- Project Parameters @object_type = 20

-- Package Parameters @object_type = 30

Set @_Step = 'Add Parameter Load_Type'
Lab520:
BEGIN TRY
--if @stage_pass_load_type_to_proc =1
EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type = 30						
	, @parameter_name = N'load_type'
	, @parameter_value = @source_load_type
END TRY
BEGIN CATCH
if @ssis_return < 0 RAISERROR('The package %s : %s : %s failed, unable to create a parameter in the catalog', 16, 1, @ssis_folder_name, @ssis_project_name, @stage_package_name)
set @ssis_return -=1
goto Lab520
END CATCH

Set @_Step = 'Add Parameter Source_Password'
set @ssis_source_password = isnull(@ssis_source_password ,'')
Lab530:
BEGIN TRY
--if @stage_pass_load_type_to_proc =1
EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type = 30						
	, @parameter_name = N'source_connection_password'
	, @parameter_value = @ssis_source_password
END TRY
BEGIN CATCH
if @ssis_return < 0 RAISERROR('The package %s : %s : %s failed, unable to create a parameter in the catalog', 16, 1, @ssis_folder_name, @ssis_project_name, @stage_package_name)
set @ssis_return -=1
goto Lab530
END CATCH

Lab540:
BEGIN TRY
--if @stage_pass_load_type_to_proc =1
EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type = 30						
	, @parameter_name = N'run_key'
	, @parameter_value = @vault_runkey
END TRY
BEGIN CATCH
if @ssis_return < 0 RAISERROR('The package %s : %s : %s failed, unable to create a parameter in the catalog', 16, 1, @ssis_folder_name, @ssis_project_name, @stage_package_name)
set @ssis_return -=1
goto Lab540
END CATCH

-- Execution Parameters @object_type = 50

Set @_Step = 'Set Logging to Verbose'
Lab560:
BEGIN TRY
EXEC [SSISDB].[catalog].[set_execution_parameter_value] @execution_id, @object_type = 50						
	, @parameter_name = N'LOGGING_LEVEL '
	, @parameter_value = 3
END TRY
BEGIN CATCH
if @ssis_return < 0 RAISERROR('The package %s : %s : %s failed, unable to create a parameter in the catalog', 16, 1, @ssis_folder_name, @ssis_project_name, @stage_package_name)
set @ssis_return -=1
goto Lab560
END CATCH


Set @_Step = 'Start Execution'
EXEC [SSISDB].[catalog].[start_execution] @execution_id

-- Check package status, and fail script if the package failed
IF 7 <> (SELECT [status] FROM [SSISDB].[catalog].[executions] WHERE execution_id = @execution_id)
RAISERROR('The package %s : %s : %s failed. Check the SSIS catalog logs for more information', 16, 1, @ssis_folder_name, @ssis_project_name, @stage_package_name)

/*--------------------------------------------------------------------------------------------------------------*/
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf
/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Loaded Object: ' + cast(@stage_qualified_name as varchar(256))

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Load Object: ' + cast(@stage_qualified_name as varchar(256))
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