Create PROCEDURE [dv_config].[dv_populate_hub_key_columns]
(
	 @vault_source_system					varchar(50)
    ,@vault_source_schema					varchar(128)
	,@vault_source_table					varchar(128)	
	,@vault_source_column_name				varchar(128)
	,@vault_hub_name						varchar(128)	= Null		
	,@vault_hub_key_column_name				varchar(128)	= Null
	,@vault_release_number					int				= 0
	,@DoGenerateError						bit				= 0
	,@DoThrowError							bit				= 1
)
AS
BEGIN
SET NOCOUNT ON;

-- Internal use variables

declare @column_key							int
	   ,@hub_key_column_key					int
	   ,@hub_key_fully_qualified			nvarchar(512)
	   ,@column_fully_qualified				nvarchar(512)


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
						+ @NEW_LINE + '    @vault_source_system          : ' + COALESCE(@vault_source_system						, '<NULL>')
						+ @NEW_LINE + '    @vault_source_schema          : ' + COALESCE(@vault_source_schema						, '<NULL>')
						+ @NEW_LINE + '    @vault_source_table           : ' + COALESCE(@vault_source_table							, '<NULL>')
						+ @NEW_LINE + '    @vault_source_column_name     : ' + COALESCE(@vault_source_column_name					, '<NULL>')
						+ @NEW_LINE + '    @vault_hub_name               : ' + COALESCE(@vault_hub_name								, '<NULL>')
						+ @NEW_LINE + '    @vault_hub_key_column_name    : ' + COALESCE(@vault_hub_key_column_name					, '<NULL>')
						+ @NEW_LINE + '    @vault_release_number         : ' + COALESCE(cast(@vault_release_number as varchar)		, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError : ' + COALESCE(CAST(@DoGenerateError AS varchar)						, '<NULL>')
						+ @NEW_LINE + '    @DoThrowError    : ' + COALESCE(CAST(@DoThrowError AS varchar)							, '<NULL>')
						+ @NEW_LINE

BEGIN TRANSACTION
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';

select @column_fully_qualified = quotename(@vault_source_system) + '.' + quotename(@vault_source_schema) + '.' + quotename(@vault_source_table) + '.' + quotename(@vault_source_column_name)
select @hub_key_fully_qualified = quotename(@vault_hub_name) + '.' + quotename(@vault_hub_key_column_name) 

SET @_Step = 'Initialise Variables';

SET @_Step = 'Create Config For Hub Key';

select @column_key = c.column_key
from [dbo].[dv_source_system] ss
  left join [dbo].[dv_source_table] st
  on st.system_key = ss.source_system_key
  left join [dbo].[dv_column] c
  on st.source_table_key = c.table_key 
where ss.source_system_name = @vault_source_system
  and st.source_table_schema = @vault_source_schema
  and st.source_table_name = @vault_source_table 
  and c.column_name = @vault_source_column_name

select @hub_key_column_key = hkc.hub_key_column_key
from [dbo].[dv_hub] h
  left join [dbo].[dv_hub_key_column] hkc
  on h.hub_key = hkc.hub_key
where h.hub_name = @vault_hub_name
  and hkc.hub_key_column_name = @vault_hub_key_column_name

select @column_key			
  	  ,@hub_key_column_key	

EXECUTE[dbo].[dv_hub_column_insert] 
   @hub_key_column_key 
  ,@column_key
  ,@vault_release_number
		


/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Populated Config for Hub Key: ' + @hub_key_fully_qualified

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Populate Config for Hub Key: ' + @hub_key_fully_qualified
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