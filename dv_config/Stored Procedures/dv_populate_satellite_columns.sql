CREATE PROCEDURE [dv_config].[dv_populate_satellite_columns]
(
	 @vault_source_system					varchar(50)
    ,@vault_source_schema					varchar(128)
	,@vault_source_table					varchar(128)	
	,@vault_satellite_name					varchar(128)	= Null
	,@vault_release_number					int				= 0
	,@vault_rerun_satellite_column_insert	bit				= 0
	,@DoGenerateError						bit				= 0
	,@DoThrowError							bit				= 1
)
AS
BEGIN
SET NOCOUNT ON;

-- Internal use variables

declare @table_fully_qualified				nvarchar(512)
	   ,@satellite_key						int
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
						+ @NEW_LINE + '    @vault_source_system                : ' + COALESCE(@vault_source_system						, '<NULL>')
						+ @NEW_LINE + '    @vault_source_schema                : ' + COALESCE(@vault_source_schema						, '<NULL>')
						+ @NEW_LINE + '    @vault_source_table                 : ' + COALESCE(@vault_source_table						, '<NULL>')
						+ @NEW_LINE + '    @vault_satellite_name               : ' + COALESCE(@vault_satellite_name						, '<NULL>')
						+ @NEW_LINE + '    @vault_rerun_satellite_column_insert: ' + COALESCE(cast(@vault_rerun_satellite_column_insert as varchar)	, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError : ' + COALESCE(CAST(@DoGenerateError AS varchar)							, '<NULL>')
						+ @NEW_LINE + '    @DoThrowError    : ' + COALESCE(CAST(@DoThrowError AS varchar)								, '<NULL>')
						+ @NEW_LINE

BEGIN TRANSACTION
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';

select @table_fully_qualified = quotename(@vault_source_system) + '.' + quotename(@vault_source_schema) + '.' + quotename(@vault_source_table)

SET @_Step = 'Initialise Variables';

SET @_Step = 'Create Config Satellite Columns';

select @satellite_key = [satellite_key]
	from [dbo].[dv_satellite] where [satellite_name] = @vault_satellite_name

select @source_table_key = st.[source_table_key] 
	from [dbo].[dv_source_table] st
	inner join [dbo].[dv_source_system] ss
	on ss.source_system_key = st.system_key
	where ss.[source_system_name]	= @vault_source_system
	  and st.[source_table_schema]	= @vault_source_schema 
	  and st.[source_table_name]	= @vault_source_table
select 1 from [dbo].[dv_satellite_column] where [satellite_key] = @satellite_key
if @@ROWCOUNT > 0  -- Satellite already has Columns attached
	begin
	if @vault_rerun_satellite_column_insert = 1
	    begin
		delete from [dbo].[dv_satellite_column] where [satellite_key] = @satellite_key
		update [dbo].[dv_column] set [satellite_col_key] = NULL where [table_key] = @source_table_key
		end
	else
		begin
			raiserror('Satellite %s has columns linked to it. Either Remove them from the Satellite or set the @vault_rerun_satellite_column_insert paramater to 1 and try again.',  16, 1, @vault_satellite_name)
		end
	end
		
declare @column_key					[int]
	   ,@column_name				[varchar](128)
	   ,@column_type				[varchar](30)
	   ,@column_length				[int]
	   ,@column_precision			[int]
	   ,@column_scale				[int]
	   ,@Collation_Name				[sysname]
	   ,@satellite_ordinal_position [int]
	   ,@satellite_col_key			[int]

declare Col_Cursor cursor forward_only for 
select [column_key]
      ,[column_name]
	  ,[column_type]
	  ,[column_length]
	  ,[column_precision]
	  ,[column_scale]
	  ,[Collation_Name]
	  ,[satellite_ordinal_position] = row_number() over (order by case when [column_name] like 'dv_%' then '___' + [column_name] else [column_name] end)	
from [dbo].[dv_column]	
 where [table_key] = @source_table_key 
open Col_Cursor
fetch next from Col_Cursor 
	into   @column_key					
		  ,@column_name				
		  ,@column_type				
		  ,@column_length				
		  ,@column_precision			
		  ,@column_scale				
		  ,@Collation_Name				
		  ,@satellite_ordinal_position	

while @@FETCH_STATUS = 0
begin								

EXECUTE @satellite_col_key = [dbo].[dv_satellite_column_insert] 
   @satellite_key				= @satellite_key
  ,@column_name					= @column_name
  ,@column_type					= @column_type
  ,@column_length				= @column_length
  ,@column_precision			= @column_precision
  ,@column_scale				= @column_scale
  ,@Collation_Name				= @Collation_Name
  ,@satellite_ordinal_position	= @satellite_ordinal_position
  ,@ref_function_key			= NULL
  ,@func_arguments              = NULL
  ,@func_ordinal_position       = 0
  ,@release_number			    = @vault_release_number

update [dbo].[dv_column] set [satellite_col_key] = @satellite_col_key
	where [column_key]			= @column_key

fetch next from Col_Cursor 
	into   @column_key					
		  ,@column_name				
		  ,@column_type				
		  ,@column_length				
		  ,@column_precision			
		  ,@column_scale				
		  ,@Collation_Name				
		  ,@satellite_ordinal_position
end
close Col_Cursor
deallocate Col_Cursor


/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Populated Columns for Satellite: ' + @vault_satellite_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Populate Columns for Satellite: ' + @vault_satellite_name
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