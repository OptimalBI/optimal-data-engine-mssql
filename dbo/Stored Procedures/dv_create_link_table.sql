
CREATE PROCEDURE [dbo].[dv_create_link_table]
(
  @vault_database				varchar(256)	= NULL
, @vault_link_name				varchar(256)	= NULL
, @recreate_flag                 char(1)		= 'N'
, @DoGenerateError               bit            = 0
, @DoThrowError                  bit			= 1
)
AS
BEGIN
SET NOCOUNT ON

declare @filegroup		varchar(256)
declare @schema			varchar(256)
declare @database		varchar(256)
declare @table_name		varchar(256)
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
						+ @NEW_LINE + '    @vault_link_name              : ' + COALESCE(@vault_link_name, '<NULL>')
						+ @NEW_LINE + '    @recreate_flag                : ' + COALESCE(@recreate_flag, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF (select count(*) from [dbo].[dv_link] where [link_database]= @vault_database and [link_name] = @vault_link_name) <> 1
			RAISERROR('Invalid link Name: %s', 16, 1, @vault_link_name);
IF isnull(@recreate_flag, '') not in ('Y', 'N') 
			RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get required Parameters'

declare @payload_columns [dbo].[dv_column_type]

select @database = [link_database]
      ,@schema = [link_schema]
	  ,@filegroup = null
from [dbo].[dv_link]
where 1=1
  and [link_database] = @vault_database
  and [link_name]	  = @vault_link_name

insert @payload_columns
select hd.[column_name]
       ,hd.[column_type]
       ,hd.[column_length]
	   ,hd.[column_precision]
	   ,hd.[column_scale]
	   ,hd.[collation_Name]
	   ,1
       ,hd.[ordinal_position]
	   ,1 
	   ,''
	   ,''
FROM [dbo].[dv_link] l
inner join [dbo].[dv_hub_link] hl
on l.link_key = hl.link_key
inner join [dbo].[dv_hub] h
on h.hub_key = hl.hub_key
cross apply fn_GetKeyDefinition(h.hub_name, 'hub') hd
where l.[link_name] = @vault_link_name

select @varobject_name = [dbo].[fn_GetObjectName](@vault_link_name, 'lnk')
select @table_name = quotename(@database) + '.' + quotename (@schema) + '.' + quotename(@varobject_name)
select @filegroup = coalesce(cast([dbo].[fn_GetDefaultValue] ('filegroup','lnk') as varchar(128)), 'Primary')

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create the Link'

EXECUTE [dbo].[dv_create_DV_table] 
   @vault_link_name
  ,@schema
  ,@database
  ,@filegroup
  ,'lnk'
  ,@payload_columns
  ,@recreate_flag
  ,@dogenerateerror
  ,@dothrowerror

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Index the Link on the Hub Keys'
select @SQL = ''
select @SQL += 'CREATE UNIQUE NONCLUSTERED INDEX ' + quotename(@varobject_name + cast(newid() as varchar(56))) 
	select @SQL += ' ON ' + @table_name + '(' + @crlf + ' '
	select @SQL = @SQL + rtrim(column_name) + @crlf +  ','
		from @payload_columns
		order by column_name
	select @SQL = left(@SQL, len(@SQL) -1) + ') ON ' + quotename(@filegroup) + @crlf 

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create The Index'
IF @_JournalOnOff = 'ON'
	SET @_ProgressText += @SQL
--print @SQL
exec (@SQL)

/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created link: ' + @table_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create link: ' + @table_name
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




