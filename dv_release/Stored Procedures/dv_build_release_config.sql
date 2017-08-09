CREATE PROCEDURE [dv_release].[dv_build_release_config]
(
  @vault_release_number			int			 = NULL
, @vault_return_change_script	bit          = 0
, @DoGenerateError              bit          = 0
, @DoThrowError                 bit			 = 1
)

AS
BEGIN
SET NOCOUNT ON
/*========================================================================================================================
Description:	This script uses [dv_release].[fn_config_table_list] () to generate a list of Config Tables, to be included in the release.
				It then calls [dv_release].[dv_build_release_config_table] for the Release Header and for each table, building up the complete release.
=========================================================================================================================*/


-- Internal use variables
DECLARE @IncludeTables table(dv_schema_name sysname, dv_table_name sysname, dv_load_order int)
insert @IncludeTables 
SELECT dv_schema_name	
      ,dv_table_name	
	  ,dv_load_order
FROM [dv_release].[fn_config_table_list] ()


DECLARE
    @vault_statement		nvarchar(max),
	@vault_change_count		int,
	@vault_start_pk			int,
	@vault_end_pk		    int,
	@release_key			int,
	@release_number			int,
	@change_count			int = 0,
	@currtable				SYSNAME,
	@currschema				SYSNAME,
	@dv_schema_name			sysname,
	@dv_table_name			sysname


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
						+ @NEW_LINE + '    @vault_return_change_script   : ' + COALESCE(cast(@vault_return_change_script as varchar(20)), '<NULL>')
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

CREATE TABLE #dv_release_build(
	[release_build_key]				[int] NOT NULL,
	[release_statement_sequence]	[int] identity(1,1) not null,
	[release_number]				[int] not null,
	[release_statement_type]		[varchar](10),
	[release_statement]				[varchar](max) NULL,
	[affected_row_count]			[int] NOT NULL)
			
SET @_Step = 'Get Release Header';
select @release_key = release_key from [dv_release].[dv_release_master] where release_number = @vault_release_number

SET @_Step = 'Initialise Release';

update [dv_release].[dv_release_master]
set [build_number]		= [build_number] + 1,
    [build_date]		= SYSDATETIMEOFFSET(),
	[build_server]		= @@SERVERNAME,
	[release_built_by]	= suser_name()
where [release_key] = @release_key

-- Extract the Release Master Details
EXECUTE [dv_release].[dv_build_release_config_table] 'dv_release', 'dv_release_master', @vault_release_number, 'release_start_datetime,release_complete_datetime,release_count', -2147483648, @vault_statement OUTPUT, @vault_change_count OUTPUT, @vault_end_pk OUTPUT
insert #dv_release_build([release_build_key], [release_number], [release_statement_type], [release_statement], [affected_row_count])values(@release_key, @vault_release_number, 'Header', @vault_statement, @vault_change_count)

-- Extract the Changes per Table
DECLARE dv_table_cursor CURSOR FOR  
SELECT dv_schema_name, dv_table_name
FROM @IncludeTables 
order by dv_load_order

OPEN dv_table_cursor
FETCH NEXT FROM dv_table_cursor INTO @dv_schema_name, @dv_table_name
SET @vault_start_pk = -2147483648 
WHILE @@FETCH_STATUS = 0   
BEGIN   
	SET @vault_change_count = 9999
	WHILE @vault_change_count > 0
	BEGIN
	   EXECUTE [dv_release].[dv_build_release_config_table] @dv_schema_name, @dv_table_name, @vault_release_number, '', @vault_start_pk, @vault_statement OUTPUT, @vault_change_count OUTPUT, @vault_end_pk OUTPUT
	   if isnull(@vault_change_count, 0) > 0
			insert #dv_release_build([release_build_key], [release_number], [release_statement_type], [release_statement], [affected_row_count]) values(@release_key, @vault_release_number, 'Table', @vault_statement, @vault_change_count)
	   SET @vault_start_pk = @vault_end_pk + 1 
	END
	FETCH NEXT FROM dv_table_cursor INTO @dv_schema_name, @dv_table_name
	SET @vault_start_pk = -2147483648
END   

CLOSE dv_table_cursor   
DEALLOCATE dv_table_cursor


delete from [dv_release].[dv_release_build] where [release_build_key] = @release_key
insert [dv_release].[dv_release_build]
SELECT [release_build_key]
      ,[release_statement_sequence]
	  ,[release_number]
	  ,[release_statement_type]
      ,[release_statement]
      ,[affected_row_count]
  FROM #dv_release_build

if @vault_return_change_script = 1
	select * from #dv_release_build

/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created Release for: ' 

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create Release for: ' 
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