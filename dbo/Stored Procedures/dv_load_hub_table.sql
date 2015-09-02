
CREATE PROCEDURE [dbo].[dv_load_hub_table]
(
  @vault_source_system		varchar(128)	= NULL
, @vault_source_schema		varchar(128)	= NULL
, @vault_source_table		varchar(128)	= NULL
, @vault_database			varchar(128)	= NULL
, @vault_hub_name			varchar(128)	= NULL
, @dogenerateerror			bit				= 0
, @dothrowerror				bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly
--declare @hub_name varchar(100) =  'AdventureWorks2014_production_productinventory'

DECLARE @dv_load_date_time_column	varchar(128)
DECLARE @dv_load_date_time			varchar(128) 
DECLARE @dv_data_source_column		varchar(128)
DECLARE @dv_data_source_key			int
DECLARE @dv_timevault_name			varchar(128)
DECLARE @hub_name					varchar(128) 
DECLARE @hub_table_name				varchar(128) 
DECLARE @default_load_date_time		varchar(128)
DECLARE @hub_load_date_time			varchar(128)
DECLARE @hub_insert_count			int

DECLARE @crlf char(2) = CHAR(13) + CHAR(10)

DECLARE @declare			nvarchar(512) = ''
DECLARE @count_rows			nvarchar(256) = ''
DECLARE @match_list			nvarchar(4000) = ''
DECLARE @value_list			nvarchar(4000) = ''
DECLARE @hub_column_list	nvarchar(4000) = ''
DECLARE @source_column_list nvarchar(4000) = ''
DECLARE @SQL1				nvarchar(4000) = ''
DECLARE @SQL2				nvarchar(4000) = '' 
DECLARE @SQL3				nvarchar(4000) = ''
DECLARE @SQL4				nvarchar(4000) = ''
DECLARE @SQL				nvarchar(4000) = ''
DECLARE @ParmDefinition		nvarchar(500);

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
						+ @NEW_LINE + '    @vault_source_system          : ' + COALESCE(@vault_source_system, '<NULL>')
						+ @NEW_LINE + '    @vault_source_schema          : ' + COALESCE(@vault_source_schema, '<NULL>')
						+ @NEW_LINE + '    @vault_source_table           : ' + COALESCE(@vault_source_table, '<NULL>')
						+ @NEW_LINE + '    @vault_database               : ' + COALESCE(@vault_database, '<NULL>')
						+ @NEW_LINE + '    @vault_hub_name               : ' + COALESCE(@vault_hub_name, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF (select count(*) from [dbo].[dv_source_system] where @vault_source_system = [source_system_name] ) <> 1
			RAISERROR('Invalid @vault_source_system: %s', 16, 1, @vault_source_system);

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'

select @declare = 'DECLARE @rowcounts TABLE(merge_action nvarchar(10));' + @crlf
select @count_rows = 'OUTPUT $action into @rowcounts;' + @crlf + 'select @insertcount = count(*) from @rowcounts;'

select @hub_table_name				= [dbo].[fn_GetObjectName](@vault_hub_name, 'hub') --from [dbo].[dv_hub] where hub_name = 'AdventureWorks2014_production_productinventory'
select @default_load_date_time		= [default_varchar] from [dbo].[dv_defaults]		where default_type = 'Global'	and default_subtype = 'DefaultLoadDateTime'
select @dv_load_date_time_column	= [column_name]		from [dbo].[dv_default_column]	where [object_type] = 'hub'		and object_column_type = 'Load_Date_Time'
select @dv_data_source_column		= [column_name]		from [dbo].[dv_default_column]	where [object_type] = 'hub'		and object_column_type = 'Data_Source'
select @dv_load_date_time			= c.column_name 
      ,@dv_data_source_key			= st.table_key
	  ,@dv_timevault_name			= s.timevault_name
from [dbo].[dv_source_system] s
inner join [dbo].[dv_source_table] st
on st.system_key = s.system_key
left join [dbo].[dv_column] c
on c.table_key = st.table_key
and c.[is_source_date] = 1
where 1=1
and s.source_system_name	= @vault_source_system
and st.source_table_schema	= @vault_source_schema
and st.source_table_name	= @vault_source_table
--and isnull(c.discard_flag, 0) <> 1
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Build SQL Components'

select @SQL1 = 'WITH wBaseSet AS (SELECT DISTINCT ' 
	  ,@source_column_list += quotename(c.[column_name]) +','
	  ,@SQL2 = 'FROM ' + quotename(s.[timevault_name]) + '.'+quotename([source_table_schema])+ '.'+quotename([source_table_name]) + ')' + @crlf 
			 + 'MERGE ' + quotename(h.[hub_database]) +'.'+quotename(h.[hub_schema])+'.'+ quotename(@hub_table_name) + ' WITH (HOLDLOCK) AS TARGET ' + @crlf
             + 'USING wBaseSet AS SOURCE' + @crlf + '  ON '
      ,@match_list += 'TARGET.' + quotename(hkc.[hub_key_column_name]) + ' = CAST(SOURCE.' + quotename(c.[column_name]) + ' as ' + [hub_key_column_type] + ')' + ' AND '
	  ,@SQL3 = @crlf + '  WHEN NOT MATCHED BY TARGET THEN ' + @crlf + 'INSERT(' + @dv_load_date_time_column + ',' + @dv_data_source_column + ',' 
	  ,@hub_column_list += hkc.hub_key_column_name + ','
	  ,@SQL4 = @crlf + 'VALUES(sysdatetimeoffset(),''' + cast(@dv_data_source_key as varchar(50)) + ''','
	  ,@value_list += 'CAST(SOURCE.' + quotename(c.[column_name]) + ' as ' + [hub_key_column_type] + ')'  + ',' 

from [dbo].[dv_hub] h
inner join [dbo].[dv_hub_key_column] hkc
on h.hub_key = hkc.hub_key
inner join [dbo].[dv_hub_column] hc
on hc.hub_key_column_key = hkc.hub_key_column_key
inner join [dbo].[dv_column] c
on c.column_key = hc.column_key
inner join [dbo].[dv_source_table] st
on c.[table_key] = st.table_key
inner join [dbo].[dv_source_system] s
on s.system_key = st.system_key
where 1=1
and h.hub_name				= @vault_hub_name
and h.hub_database			= @vault_database
and s.source_system_name	= @vault_source_system
and st.source_table_schema	= @vault_source_schema
and st.source_table_name	= @vault_source_table
and isnull(c.discard_flag, 0) <> 1
ORDER BY hkc.hub_key_ordinal_position 
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Combine the SQL Components'

select @SQL = replace(
          @declare
        + @SQL1
		+ left(@source_column_list, len(@source_column_list) -1) 
		+ @SQL2
		+ left(@match_list, len(@match_list) -4)
		+ @SQL3
		+ @hub_column_list + ')'
		+ @SQL4
		+ @value_list + ')'
		+ @count_rows
		, ',)', ')')



/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Load The Hub'
IF @_JournalOnOff = 'ON'
	SET @_ProgressText += @SQL
SET @ParmDefinition = N'@insertcount int OUTPUT';
 --print @sql 

EXECUTE sp_executesql @SQL, @ParmDefinition, @insertcount = @hub_insert_count OUTPUT;
--select @hub_insert_count

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Loaded Object: ' + @hub_table_name + ' (' + cast(@hub_insert_count as varchar(50)) + ' New Keys Added)'

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Load Object: ' + @hub_table_name
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