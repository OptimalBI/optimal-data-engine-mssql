
CREATE PROCEDURE [dbo].[dv_create_stage_table]
(
  @vault_source_unique_name            varchar(128)   = NULL
, @recreate_flag                 char(1)		= 'N'
, @DoGenerateError               bit            = 0
, @DoThrowError                  bit			= 1
)
AS
BEGIN
SET NOCOUNT ON
-- Global Defaults
DECLARE @crlf				char(2)	= CHAR(13) + CHAR(10)
-- Stage Table
DECLARE @filegroup				VARCHAR(256)
       ,@schema					VARCHAR(256)
       ,@database				VARCHAR(256)
	   ,@load_type				VARCHAR(50)
       ,@table_name				VARCHAR(256)
       ,@table_qualified_name	VARCHAR(512)
	   ,@pk_name				VARCHAR(256)
       ,@is_columnstore			BIT
	   ,@is_compressed			BIT

-- Working Storage

DECLARE  @payload_columns           [dbo].[dv_column_type]
        ,@default_columns			[dbo].[dv_column_type]
        ,@SQL						nvarchar(max) = ''
		,@varobject_name			varchar(128)

-- Log4TSQL Journal Constants 
DECLARE @SEVERITY_CRITICAL      SMALLINT = 1;
DECLARE @SEVERITY_SEVERE        SMALLINT = 2;
DECLARE @SEVERITY_MAJOR         SMALLINT = 4;
DECLARE @SEVERITY_MODERATE      SMALLINT = 8;
DECLARE @SEVERITY_MINOR         SMALLINT = 16;
DECLARE @SEVERITY_CONCURRENCY   SMALLINT = 32;
DECLARE @SEVERITY_INFORMATION   SMALLINT = 256;
DECLARE @SEVERITY_SUCCESS       SMALLINT = 512;
DECLARE @SEVERITY_DEBUG         SMALLINT = 1024;
DECLARE @NEW_LINE               CHAR(1)  = CHAR(10);

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
						+ @NEW_LINE + '    @vault_source_unique_name     : ' + COALESCE(@vault_source_unique_name, '<NULL>')
						+ @NEW_LINE + '    @recreate_flag                : ' + COALESCE(@recreate_flag, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF (select count(*) from [dbo].[dv_source_table] where source_unique_name = @vault_source_unique_name) <> 1
			RAISERROR('Invalid Stage Source Name: %s', 16, 1, @vault_source_unique_name);
IF isnull(@recreate_flag, '') not in ('Y', 'N') 
			RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get required Parameters'

select @database	= sd.stage_database_name
      ,@schema		= sc.stage_schema_name
	  ,@load_type	= st.load_type
	  ,@table_name	= st.stage_table_name
	  ,@is_columnstore = st.is_columnstore
	  ,@is_compressed = st.is_compressed
from [dbo].[dv_source_table] st		
left join [dbo].[dv_stage_schema] sc on sc.stage_schema_key = st.stage_schema_key
left join [dbo].[dv_stage_database] sd on sd.stage_database_key = sc.stage_database_key
where 1=1
and st.source_unique_name	= @vault_source_unique_name
-- Find the Mangement Columns
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
and [object_type] = 'stg'
and [object_column_type] <> 'Object_Key'
-- Add Management columns for CDC Load.
if @load_type = 'ODEcdc'
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
	and [object_type] = 'CdcStgODE'
	and [object_column_type] <> 'Object_Key'

else if @load_type = 'MSSQLcdc'
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
	and [object_type] = 'CdcStgMSSQL'
	and [object_column_type] <> 'Object_Key'

-- Find the Payload Columns
insert @payload_columns
select  c.[column_name]
       ,c.[column_type]
       ,c.[column_length]
	   ,c.[column_precision]
	   ,c.[column_scale]
	   ,c.[collation_name]
	   ,c.[source_ordinal_position]
       ,c.[source_ordinal_position]
	   ,c.[source_ordinal_position]
	   ,ISNULL(c.[derived_value], '')
	   ,ISNULL(c.[is_derived], 0)
  FROM [dbo].[dv_source_table] st
  inner join [dbo].[dv_column] c
  on st.source_table_key = c.table_key
  where 1=1
  and st.source_unique_name	= @vault_source_unique_name
  and c.[column_name] not in (select [column_name] from [dbo].[dv_default_column] where object_type = 'Stg' and object_column_type <> 'Object_Key')

select @varobject_name = [dbo].[fn_get_object_name](@table_name, 'stg')
select @table_qualified_name = quotename(@database) + '.' + quotename (@schema) + '.' + quotename(@varobject_name)
select @filegroup = coalesce(cast([dbo].[fn_get_default_value] ('filegroup','stg') as varchar(128)), 'Primary')

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create The Table'
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'If Recreate then Drop Existing Table'

IF @recreate_flag = 'Y'
BEGIN
	select @SQL += 'IF EXISTS (select 1 from ' + quotename(@database) + '.INFORMATION_SCHEMA.TABLES where TABLE_TYPE = ''BASE TABLE'' and TABLE_SCHEMA = ''' + @schema + ''' and TABLE_NAME = ''' + @varobject_name + ''')' + @crlf + ' '
	select @SQL += 'DROP TABLE ' + @table_qualified_name + @crlf + ' '
END
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create Table Statement'
select @SQL += 'CREATE TABLE ' + @table_qualified_name + '(' + @crlf + ' '

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Add the Columns'
--Primary Key
select @SQL = @SQL + [column_name] + dbo.[fn_build_column_definition]('',[column_type], [column_length], [column_precision], [column_scale], [Collation_Name], 0, NULL, 0, 1, 0, 1) + @crlf + ',' 
from [fn_get_key_definition](@table_name, 'stg')

--Technical Columns
select @SQL = @SQL + QUOTENAME([column_name]) + ' ' + dbo.[fn_build_column_definition]('',[column_type], [column_length], [column_precision], [column_scale], [Collation_Name], 0, NULL, 1, 0, 0, 1) + @crlf + ',' 
from
(select *
from @default_columns) a
order by [source_ordinal_position]

--Payload Columns
select @SQL = @SQL + QUOTENAME([column_name]) + ' ' + dbo.[fn_build_column_definition]('',[column_type], [column_length], [column_precision], [column_scale], [Collation_Name], [has_default], [default_value], 1, 0, 0, 1) + @crlf + ',' 
from
(select *
from @payload_columns) a
order by [satellite_ordinal_position], [column_name]

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Add the Primary Key'
if @is_columnstore = 0
	begin
	select @pk_name = [column_name] from [fn_get_key_definition](@table_name, 'stg')
	select @SQL += 'PRIMARY KEY CLUSTERED (' + @pk_name + ') ON ' + quotename(@filegroup) + @crlf
	end
select @SQL += ') '	
select @SQL += ' ON ' + quotename(@filegroup) 
if @is_columnstore = 0  -- columnstore takes precedence if both switches are set.
	if @is_compressed = 1
		select @SQL += ' WITH ( DATA_COMPRESSION = PAGE ) '
select @SQL += ';' + @crlf

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Index the Table'
if @is_columnstore = 1
	--if @is_compressed <> 1
		begin
		select @SQL += 'CREATE CLUSTERED COLUMNSTORE INDEX ' + quotename('CCX__' + @table_name + cast(newid() as varchar(56)))
		select @SQL += ' ON ' + @table_qualified_name + @crlf + ' ' 
		end
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create The Table'
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @SQL + @crlf

--PRINT LEFT(@SQL, 3000)
--PRINT '***************************'
--PRINT SUBSTRING(@SQL, 3001, 3000)
EXEC (@SQL)

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

--IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created Source Table: ' + @table_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create Source Table: ' + @table_name
IF (XACT_STATE() = -1) -- uncommitable transaction
OR (@@TRANCOUNT > 0 AND XACT_STATE() != 1) -- undocumented uncommitable transaction
	BEGIN
		--ROLLBACK TRAN;
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