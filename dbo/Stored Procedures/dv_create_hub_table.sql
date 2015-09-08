CREATE PROCEDURE [dbo].[dv_create_hub_table]
(
  @vault_database			varchar(128)	= NULL
, @vault_hub_name			varchar(128)	= NULL
, @recreate_flag                 char(1)		= 'N'
, @dogenerateerror			bit				= 0
, @dothrowerror				bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

declare @filegroup		varchar(256)
declare @schema			varchar(256)
declare @database		varchar(256)
declare @table_name		varchar(256)
declare @pk_name		varchar(256)
declare @crlf			char(2) = CHAR(13) + CHAR(10)
declare @SQL			varchar(4000) = ''
declare @varobject_name varchar(128)

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
						+ @NEW_LINE + '    @vault_database               : ' + COALESCE(@vault_database, '<NULL>')
						+ @NEW_LINE + '    @vault_hub_name               : ' + COALESCE(@vault_hub_name, '<NULL>')
						+ @NEW_LINE + '    @recreate_flag                : ' + COALESCE(@recreate_flag, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF (select count(*) from [dbo].[dv_hub] where [hub_database] = @vault_database and [hub_name] = @vault_hub_name) <> 1
			RAISERROR('Invalid Hub Name: %s', 16, 1, @vault_hub_name);
IF isnull(@recreate_flag, '') not in ('Y', 'N') 
			RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get required Parameters'

declare @payload_columns [dbo].[dv_column_type]

select @database = [hub_database]
      ,@schema = [hub_schema]
	  ,@filegroup = null
from [dbo].[dv_hub]
where 1=1
   and [hub_database] = @vault_database
   and [hub_name]	  = @vault_hub_name

insert @payload_columns
select  hkc.[hub_key_column_name]
       ,hkc.[hub_key_column_type]
       ,hkc.[hub_key_column_length]
	   ,hkc.[hub_key_column_precision]
	   ,hkc.[hub_key_column_scale]
	   ,hkc.[hub_key_collation_name]
	   ,hkc.[hub_key_ordinal_position]
       ,hkc.[hub_key_ordinal_position]
	   ,hkc.[hub_key_ordinal_position]
	   ,''
	   ,''
  FROM [dbo].[dv_hub] h
  inner join [dbo].[dv_hub_key_column] hkc
  on h.hub_key = hkc.hub_key
  where 1=1
   and h.[hub_database] = @vault_database
   and h.[hub_name]		= @vault_hub_name

select @varobject_name = [dbo].[fn_GetObjectName](@vault_hub_name, 'hub')
select @table_name = quotename(@database) + '.' + quotename (@schema) + '.' + quotename(@varobject_name)
select @filegroup = coalesce(cast([dbo].[fn_GetDefaultValue] ('filegroup','hub') as varchar(128)), 'Primary')

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create The Hub'

EXECUTE [dbo].[dv_create_DV_table] 
   @vault_hub_name
  ,@schema
  ,@database
  ,@filegroup
  ,'Hub'
  ,@payload_columns
  ,@recreate_flag
  ,@dogenerateerror
  ,@dothrowerror

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Index the Hub on the Business Key'
select @SQL = ''
select @SQL += 'CREATE UNIQUE NONCLUSTERED INDEX ' + quotename(@varobject_name + cast(newid() as varchar(56))) 
	select @SQL += ' ON ' + @table_name + '(' + @crlf + ' '
	select @SQL = @SQL + rtrim(quotename(column_name)) + @crlf +  ','
		from @payload_columns
		order by bk_ordinal_position
	select @SQL = left(@SQL, len(@SQL) -1) + ') ON ' + quotename(@filegroup) + @crlf

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create The Index'
IF @_JournalOnOff = 'ON'
	SET @_ProgressText += @SQL
--print @SQL
exec (@SQL)

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created Hub: ' + @table_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create Hub: ' + @table_name
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