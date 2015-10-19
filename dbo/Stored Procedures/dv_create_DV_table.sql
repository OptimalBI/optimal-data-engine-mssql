CREATE PROCEDURE [dbo].[dv_create_DV_table]
(
  @object_name                   varchar(128)   = NULL
, @object_schema				 varchar(128)   = NULL
, @object_database				 varchar(128)   = NULL
, @object_filegroup              varchar(128)   = NULL
, @object_type					 varchar(30)    = NULL
, @payload_columns			     [dbo].[dv_column_type] READONLY
, @sat_is_columnstore			 bit			= 0
, @recreate_flag                 char(1)		= 'N'
, @dogenerateerror               bit            = 0
, @dothrowerror                  bit			= 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly
--declare @hub_name varchar(100) =  'AdventureWorks2014_production_productinventory'

declare @varobject_name			varchar(128)
--declare @filegroup				varchar(128)
declare @schema					varchar(128)
declare @database				varchar(128)
declare @default_columns		dv_column_type
declare @table_name				varchar(128)
declare @pk_name				varchar(128)
declare @payload_columns_string nvarchar(max)
declare @crlf					char(2) = CHAR(13) + CHAR(10)
declare @SQL					nvarchar(max) = ''

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

select @payload_columns_string =''
select @payload_columns_string +=  [column_name] + ',' + 'column_type: ' +[column_type] + ', column_length:' + isnull(cast([column_length] as varchar(128)), '<NULL>') + ', column_precision:' + isnull(cast([column_precision] as varchar(128)),'<NULL>') + ', column_scale;' + isnull(cast([column_scale] as varchar(128)), '<NULL>') + ', Collation_Name:' + isnull([Collation_Name], '<NULL>') + @crlf
      from @payload_columns
-- set the Parameters for logging:

SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @object_name                  : ' + COALESCE(@object_name, '<NULL>')
						+ @NEW_LINE + '    @object_schema                : ' + COALESCE(@object_schema, '<NULL>')
						+ @NEW_LINE + '    @object_database              : ' + COALESCE(@object_database, '<NULL>')
						+ @NEW_LINE + '    @object_filegroup             : ' + COALESCE(@object_filegroup, '<NULL>')
						+ @NEW_LINE + '    @object_type                  : ' + COALESCE(@object_type, '<NULL>')
						+ @NEW_LINE + '    @payload_columns              : ' + COALESCE(@payload_columns_string, '<NULL>')
						+ @NEW_LINE + '    @sat_is_columnstore           : ' + COALESCE(CAST(@sat_is_columnstore AS varchar), '<NULL>')
						+ @NEW_LINE + '    @recreate_flag                : ' + COALESCE(@recreate_flag, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF isnull(@object_name, '')		= '' RAISERROR('No @object_name: %s', 16, 1, @object_name);
IF isnull(@object_schema, '')	= '' RAISERROR('No @object_schema: %s', 16, 1, @object_schema);
IF isnull(@object_database, '')	= '' RAISERROR('No @object_database: %s', 16, 1, @object_database);
IF isnull(@object_database, '')	= '' RAISERROR('No @object_filegroup: %s', 16, 1, @object_filegroup);
IF isnull(@object_type, '')	not in('Hub', 'Lnk', 'Sat') RAISERROR(' @object_type: %s: Valid Values: Hub, Lnk or Sat', 16, 1, @object_type);
IF isnull(@payload_columns_string, '')	= '' RAISERROR('No @payload_columns: %s', 16, 1, @payload_columns_string);
IF isnull(@recreate_flag, '') not in ('Y', 'N') RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'

select @varobject_name = [dbo].[fn_get_object_name](@object_name, @object_type)
select @table_name = quotename(@object_database) + '.' + quotename (@object_schema) + '.' + quotename(@varobject_name)

insert @default_columns	
select  [column_name]
       ,[column_type]
       ,[column_length]
	   ,[column_precision]
	   ,[column_scale]
	   ,[collation_Name]
	   ,-1
       ,[ordinal_position]
	   ,-1
	   ,''
	   ,''
from [dbo].[dv_default_column]
where 1=1
and [object_type] = @object_type
and [object_column_type] <> 'Object_Key'

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'If Recreate then Drop Existing Table'

IF @recreate_flag = 'Y'
BEGIN
	select @SQL += 'IF EXISTS (select 1 from ' + quotename(@object_database) + '.INFORMATION_SCHEMA.TABLES where TABLE_TYPE = ''BASE TABLE'' and TABLE_SCHEMA = ''' + @object_schema + ''' and TABLE_NAME = ''' + @varobject_name + ''')' + @crlf + ' '
	select @SQL += 'DROP TABLE ' + @table_name + @crlf + ' '
END
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create Table Statement'
select @SQL += 'CREATE TABLE ' + @table_name + '(' + @crlf + ' '

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Add the Columns'
--1. Primary Key
select @SQL = @SQL + column_name + dbo.[fn_build_column_definition]([column_type], [column_length], [column_precision], [column_scale], [Collation_Name], 0, 1) + @crlf + ',' 
from [fn_get_key_definition](@object_name, @object_type)

--Payload
select @SQL = @SQL + column_name + ' ' + dbo.[fn_build_column_definition]([column_type], [column_length], [column_precision], [column_scale], [Collation_Name], 1, 0) + @crlf + ',' 
from
(select *
from @default_columns) a
order by source_ordinal_position
select @SQL = @SQL + column_name + ' ' + dbo.[fn_build_column_definition]([column_type], [column_length], [column_precision], [column_scale], [Collation_Name], 1, 0) + @crlf + ',' 
from
(select *
from @payload_columns) a
order by satellite_ordinal_position, column_name

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Add the Primary Key'
if @sat_is_columnstore = 0
	begin
	select @pk_name = column_name from [fn_get_key_definition](@object_name, @object_type)
	select @SQL += 'PRIMARY KEY CLUSTERED (' + @pk_name + ') ON ' + quotename(@object_filegroup) + @crlf
	end
	
select @SQL += ') ON ' + quotename(@object_filegroup) + ';' + @crlf

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create The Table'
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @SQL + @crlf
--print @SQL
exec (@SQL)

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created Object: ' + @table_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create Object: ' + @table_name
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