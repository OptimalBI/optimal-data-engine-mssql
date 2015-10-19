
CREATE PROCEDURE [dv_config].[dv_populate_satellite]
(
	 @vault_satellite_name					varchar(128)	= Null
    ,@vault_link_hub_flag					char(1)			= null
	,@vault_hub_link_name					varchar(128)	= Null
	,@vault_satellite_database				varchar(128)	= Null
	,@vault_duplicate_removal_threshold		bit				= 0
	,@vault_is_columnstore					bit				= 0
	,@vault_rerun_satellite_insert			bit				= 0
	,@vault_release_number					int				= 0
	,@DoGenerateError						bit				= 0
	,@DoThrowError							bit				= 1
)
AS
BEGIN
SET NOCOUNT ON;

-- Internal use variables

declare @satellite_key						int
       ,@hub_key							int
       ,@link_key							int
	   ,@link_hub_satellite_flag			char(1)
	   ,@satellite_name						varchar(128)
	   ,@satellite_abbreviation				varchar(4)
	   ,@satellite_schema					varchar(128)	
	   ,@satellite_database					varchar(128)
	   ,@duplicate_removal_threshold		int
	   ,@is_columnstore						bit
	   ,@release_key						bit
	   ,@source_table_key					int


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
						+ @NEW_LINE + '    @vault_satellite_name               : ' + COALESCE(@vault_satellite_name						, '<NULL>')
						+ @NEW_LINE + '    @vault_link_hub_flag                : ' + COALESCE(@vault_link_hub_flag						, '<NULL>')
						+ @NEW_LINE + '    @vault_satellite_database           : ' + COALESCE(@vault_satellite_database					, '<NULL>')
						+ @NEW_LINE + '    @vault_duplicate_removal_threshold  : ' + COALESCE(cast(@vault_satellite_name as varchar)	, '<NULL>')
						+ @NEW_LINE + '    @vault_rerun_satellite_insert       : ' + COALESCE(cast(@vault_rerun_satellite_insert as varchar), '<NULL>')
						+ @NEW_LINE + '    @vault_release_number               : ' + COALESCE(CAST(@vault_release_number AS varchar)	, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError : ' + COALESCE(CAST(@DoGenerateError AS varchar)							, '<NULL>')
						+ @NEW_LINE + '    @DoThrowError    : ' + COALESCE(CAST(@DoThrowError AS varchar)								, '<NULL>')
						+ @NEW_LINE

BEGIN TRANSACTION
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';

SET @_Step = 'Initialise Variables';

SET @_Step = 'Create Config Satellite Columns';

select @satellite_key				= [satellite_key]
	from [dbo].[dv_satellite] where [satellite_name] = @vault_satellite_name
 
if @satellite_key is not null  -- Satellite Exists - keeping it simple for now - just blow the sat away and redo it.
	begin
	if @vault_rerun_satellite_insert = 1
	    begin
		delete from [dbo].[dv_satellite_column] where [satellite_key] = @satellite_key
		delete from [dbo].[dv_satellite] where [satellite_key] = @satellite_key
		end
	else
		begin
			raiserror('Satellite %s Exists. Either Remove the Satellite or set the @vault_rerun_satellite_column_insert paramater to 1 and try again.',  16, 1, @vault_satellite_name)
		end
	end

if @vault_link_hub_flag = 'H'
	select @hub_key = [hub_key]
	      ,@link_key = 0
	from [dbo].[dv_hub]
	where [hub_name] = @vault_hub_link_name
else
	select @link_key = [link_key]
	      ,@hub_key = 0
	from [dbo].[dv_link]
	where [link_name] = @vault_hub_link_name
	
select @satellite_abbreviation	= [dbo].[fn_get_next_abbreviation] ()
select @satellite_schema		= cast([dbo].[fn_get_default_value]('Schema', 'Sat') as varchar)	
	
EXECUTE  [dbo].[dv_satellite_insert] 
   @hub_key						= @hub_key 
  ,@link_key					= @link_key
  ,@link_hub_satellite_flag		= @vault_link_hub_flag
  ,@satellite_name				= @vault_satellite_name
  ,@satellite_abbreviation		= @satellite_abbreviation	
  ,@satellite_schema			= @satellite_schema
  ,@satellite_database			= @vault_satellite_database
  ,@duplicate_removal_threshold = @vault_duplicate_removal_threshold
  ,@is_columnstore				= @vault_is_columnstore
  ,@release_number				= @vault_release_number



/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Populated Satellite: ' + @vault_satellite_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Populate Satellite: ' + @vault_satellite_name
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