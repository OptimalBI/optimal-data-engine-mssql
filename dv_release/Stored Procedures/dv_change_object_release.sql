
CREATE PROCEDURE [dv_release].[dv_change_object_release]
(
  @vault_config_table		varchar(128)	= NULL
, @vault_config_table_key   int				= NULL
, @vault_old_release		int				= NULL
, @vault_new_release		int				= NULL
, @dogenerateerror			bit				= 0
, @dothrowerror				bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

--declare @filegroup		varchar(256)
--declare @schema			varchar(256)
--declare @database		varchar(256)
--declare @table_name		varchar(256)
--declare @pk_name		varchar(256)
--declare @crlf			char(2) = CHAR(13) + CHAR(10)
declare @SQL							nvarchar(max) = ''
declare @new_release_key				int
declare @old_release_key				int
declare @vault_config_table_key_name	varchar(128)

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
						+ @NEW_LINE + '    @vault_config_table           : ' + COALESCE(@vault_config_table, '<NULL>')
						+ @NEW_LINE + '    @vault_config_table_key       : ' + COALESCE(cast(@vault_config_table_key as varchar(20)), '<NULL>')
						+ @NEW_LINE + '    @vault_old_release            : ' + COALESCE(cast(@vault_old_release as varchar(20)), '<NULL>')
						+ @NEW_LINE + '    @vault_new_release            : ' + COALESCE(cast(@vault_new_release as varchar(20)), '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

DECLARE @IncludeTables table(dv_schema_name sysname, dv_table_name sysname, dv_key_name sysname)
insert @IncludeTables 
SELECT dv_schema_name	
      ,dv_table_name	
	  ,dv_key_name
FROM [dv_release].[fn_ConfigTableList] ()

IF (select count(*) from @IncludeTables where dv_table_name = @vault_config_table) <> 1
			RAISERROR('Invalid Config Table Name Selected: %s', 16, 1, @vault_config_table);

select @old_release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @vault_old_release
if @@rowcount <> 1 RAISERROR('Invalid Old Release Number Selected: %i', 16, 1, @vault_old_release)
select @new_release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @vault_new_release
if @@rowcount <> 1 RAISERROR('Invalid New Release Number Selected: %i', 16, 1, @vault_new_release)
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Required Parameters'

select @vault_config_table_key_name = dv_key_name from @IncludeTables where dv_table_name = @vault_config_table
set @SQL = 'UPDATE [dbo].#config_table# SET [release_key] = #new_release_key# WHERE [release_key] = #old_release_key# and #vault_config_table_key_name# = #vault_config_table_key#'
set @SQL = replace(@SQL, '#config_table#', quotename(@vault_config_table))
set @SQL = replace(@SQL, '#new_release_key#', format(@new_release_key, '000000000'))
set @SQL = replace(@SQL, '#old_release_key#', format(@old_release_key, '000000000'))
set @SQL = replace(@SQL, '#vault_config_table_key_name#', @vault_config_table_key_name)
set @SQL = replace(@SQL, '#vault_config_table_key#', format(@vault_config_table_key, '000000000'))
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Move the Object to the New Release'
--print @SQL
exec (@SQL)
/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Moved ' + @vault_config_table + '(' +  format(@vault_config_table_key, '00000000') + ')' 

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Move ' + @vault_config_table + '(' +  format(@vault_config_table_key, '00000000') + ')'
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