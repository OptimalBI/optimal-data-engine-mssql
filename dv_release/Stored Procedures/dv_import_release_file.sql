CREATE PROCEDURE [dv_release].[dv_import_release_file] 
(
  @vault_file_location			varchar(256)	=''
, @vault_file_name				varchar(256)	=''
, @DoGenerateError              bit				= 0
, @DoThrowError                 bit				= 1
)
AS
BEGIN
SET NOCOUNT ON
-- Internal use variables
declare @xml xml
       ,@sql					nvarchar(max) = ''
	   ,@parmdefinition			nvarchar(500)
	   ,@release_build_key		int
	   ,@temp_table_name		varchar(256)
	   ,@full_file_name			varchar(256)

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
						+ @NEW_LINE + '    @vault_file_location			 : ' + COALESCE(@vault_file_location, '<NULL>')
						+ @NEW_LINE + '    @vault_file_name		         : ' + COALESCE(cast(@vault_file_name as varchar(20)), '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';

SET @_Step = 'Initialise Variables';

if isnull(@vault_file_location, '') = ''
	set @vault_file_location = 'C:\bcp\'

set @full_file_name = @vault_file_location + '\' + @vault_file_name		
set @full_file_name = replace (@full_file_name, '\\', '\')
-- Create Temp Table Name
select @temp_table_name = '##temp_001_' + replace(cast(newid() as varchar(50)), '-', '')
-- Build a Global Temp Table
set @sql = 'create table #table_name# (XmlCol xml)'
set @sql = replace (@sql, '#table_name#', @temp_table_name)
exec(@sql)
--Load the File into the Golbal Temp Table
set @sql = 'INSERT INTO #temp_table_name#(XmlCol) SELECT * FROM OPENROWSET(BULK ''#filename#'',SINGLE_CLOB) AS x;' 
set @sql = replace(@sql, '#temp_table_name#', @temp_table_name)
set @sql = replace(@sql, '#filename#', @full_file_name)
exec(@sql)
-- return the Build as an XML variable
SET @parmdefinition = N'@XMLData xml OUTPUT'
set @sql = 'select @XMLData = XmlCol from #temp_table_name#'
set @sql = replace(@sql, '#temp_table_name#', @temp_table_name)
EXECUTE sp_executesql @sql, @ParmDefinition, @XMLData = @xml OUTPUT;
--Clear the release out of the master file
SELECT @release_build_key = Tbl.Col.value('release_build_key[1]', 'varchar(4000)') 
FROM   @xml.nodes('//statement') Tbl(Col)
select @release_build_key
delete from [dv_release].[dv_release_build] where release_build_key = @release_build_key
--Import the release into the Build Table, Ready to apply
insert [dv_release].[dv_release_build]
   SELECT  
       Tbl.Col.value('release_build_key[1]', 'varchar(4000)') as release_build_key, 
	   Tbl.Col.value('release_statement_sequence[1]', 'varchar(4000)') as release_statement_sequence, 
	   Tbl.Col.value('release_number[1]', 'varchar(4000)') as release_number,
	   Tbl.Col.value('release_statement_type[1]', 'varchar(4000)') as release_statement_type,
	   Tbl.Col.value('release_statement[1]', 'varchar(max)') as release_statement,
	   Tbl.Col.value('affected_row_count[1]', 'varchar(4000)') as affected_row_count
FROM   @xml.nodes('//statement') Tbl(Col) 

/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Imported Release File: ' + @full_file_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Import Release File: ' + @full_file_name
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