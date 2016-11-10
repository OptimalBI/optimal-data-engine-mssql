CREATE PROCEDURE [dbo].[dv_load_hub_table]
(
  @vault_source_system		varchar(128)	= NULL
, @vault_source_schema		varchar(128)	= NULL
, @vault_source_table		varchar(128)	= NULL
--, @vault_database			varchar(128)	= NULL
, @vault_hub_name			varchar(128)	= NULL
, @dogenerateerror			bit				= 0
, @dothrowerror				bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly

DECLARE @dv_load_date_time_column	varchar(128)
DECLARE @dv_load_date_time			varchar(128) 
DECLARE @dv_data_source_column		varchar(128)
DECLARE @default_load_date_time		varchar(128)
DECLARE @dv_data_source_key			int
DECLARE @dv_timevault_name			varchar(128)

DECLARE @hub_name					varchar(128) 
DECLARE @hub_table_name				varchar(128)
DECLARE @hub_key_column_name		varchar(128)
DECLARE @hub_load_date_time			varchar(128)
DECLARE @hub_database				varchar(128)
DECLARE @hub_schema					varchar(128)

DECLARE @link_key_column_name		varchar(128)
DECLARE @link_key_column_key		int
DECLARE @link_key					int

DECLARE @column_name				varchar(128)
DECLARE @hub_column_definition		varchar(128)
DECLARE @hub_column_definition_cast	varchar(128)
DECLARE @source_column_definition	varchar(128)

DECLARE @hub_insert_count			int
DECLARE @loop_count					int
DECLARE @current_link_key_column_key int

DECLARE @crlf char(2) = CHAR(13) + CHAR(10)

DECLARE @match_list			nvarchar(4000) = ''
DECLARE @value_list			nvarchar(4000) = ''
DECLARE @hub_column_list	nvarchar(4000) = ''
DECLARE @source_column_list nvarchar(4000) = ''
DECLARE @SQL1				nvarchar(4000) = ''
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
						--+ @NEW_LINE + '    @vault_database               : ' + COALESCE(@vault_database, '<NULL>')
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


select @hub_table_name				= [dbo].[fn_get_object_name](@vault_hub_name, 'hub') 
select @default_load_date_time		= [default_varchar] from [dbo].[dv_defaults]		where default_type = 'Global'	and default_subtype = 'DefaultLoadDateTime'
select @dv_load_date_time_column	= [column_name]		from [dbo].[dv_default_column]	where [object_type] = 'hub'		and object_column_type = 'Load_Date_Time'
select @dv_data_source_column		= [column_name]		from [dbo].[dv_default_column]	where [object_type] = 'hub'		and object_column_type = 'Data_Source'
select @dv_load_date_time			= c.column_name 
      ,@dv_data_source_key			= st.[source_table_key]
	  ,@dv_timevault_name			= s.timevault_name
from [dbo].[dv_source_system] s
inner join [dbo].[dv_source_table] st		on st.system_key = s.[source_system_key]
left join [dbo].[dv_column] c				on c.table_key = st.[source_table_key]
										   and c.[is_source_date] = 1
										   and isnull(c.is_retired, 0) <> 1
where 1=1
and s.source_system_name	= @vault_source_system
and st.source_table_schema	= @vault_source_schema
and st.source_table_name	= @vault_source_table
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Build SQL Components'


DECLARE cur_hub_column CURSOR FOR  
select c.column_name
      ,hkc.hub_key_column_name
	  ,link_key_column_key = isnull(hc.link_key_column_key, 0)
	  ,s.timevault_name
	  ,h.hub_database
	  ,h.hub_schema
	  ,[dbo].[fn_build_column_definition] ('',[hub_key_column_type],[hub_key_column_length],[hub_key_column_precision],[hub_key_column_scale],[hub_key_Collation_Name],0,0,0,0)
	  ,[dbo].[fn_build_column_definition] ('',[column_type],[column_length],[column_precision],[column_scale],[Collation_Name],0,0, 0,0)
	  ,[dbo].[fn_build_column_definition] (quotename(c.column_name),[hub_key_column_type],[hub_key_column_length],[hub_key_column_precision],[hub_key_column_scale],[hub_key_Collation_Name],0,0, 1,0)
	  
from [dbo].[dv_hub] h
inner join [dbo].[dv_hub_key_column] hkc
on h.hub_key = hkc.hub_key
inner join [dbo].[dv_hub_column] hc
on hc.hub_key_column_key = hkc.hub_key_column_key
inner join [dbo].[dv_column] c
on c.column_key = hc.column_key
inner join [dbo].[dv_source_table] st
on c.[table_key] = st.[source_table_key]
inner join [dbo].[dv_source_system] s
on s.[source_system_key] = st.system_key
where 1=1
and h.hub_name				= @vault_hub_name
--and h.hub_database			= @vault_database
and s.source_system_name	= @vault_source_system
and st.source_table_schema	= @vault_source_schema
and st.source_table_name	= @vault_source_table
and isnull(c.is_retired, 0) <> 1
order by link_key_column_key, c.column_name  --NB order is vital for the following loop to work!

set @loop_count = 0
set @SQL1 = 'DECLARE @rowcounts TABLE(merge_action nvarchar(10));' + @crlf + 'WITH wBaseSet AS (' + @crlf

OPEN cur_hub_column   
FETCH NEXT FROM cur_hub_column INTO  @column_name		   
							   ,@hub_key_column_name
							   ,@link_key_column_key
							   ,@dv_timevault_name
							   ,@hub_database
							   ,@hub_schema
							   ,@hub_column_definition
							   ,@source_column_definition
							   ,@hub_column_definition_cast
WHILE @@FETCH_STATUS = 0
BEGIN   
       set @current_link_key_column_key = @link_key_column_key
	   set @SQL1 += 'SELECT DISTINCT ' 
	   set @source_column_list = ''
	   set @loop_count += 1	   
	   while @current_link_key_column_key = @link_key_column_key and @@FETCH_STATUS = 0
	   BEGIN
			select @source_column_list +=  @crlf +
			       case when @hub_column_definition = @source_column_definition
				        then quotename(@column_name)
					    --else ' CAST(' + quotename(@column_name) + ' AS ' + left(@hub_column_definition, len(@hub_column_definition) -5) + ')'
						else @hub_column_definition_cast 
					    end + 
			       ' AS ' + quotename(@hub_key_column_name) + ','
			if @loop_count = 1
			begin
				select @match_list += @crlf +' TARGET.' + quotename(@hub_key_column_name) + ' = SOURCE.' + quotename(@hub_key_column_name)  + ' AND '
				select @hub_column_list  += @crlf +' SOURCE.' + quotename(@hub_key_column_name) + ','
			end
			FETCH NEXT FROM cur_hub_column INTO  @column_name		   
							   ,@hub_key_column_name
							   ,@link_key_column_key
							   ,@dv_timevault_name
							   ,@hub_database
							   ,@hub_schema
							   ,@hub_column_definition
							   ,@source_column_definition							   
							   ,@hub_column_definition_cast
		END
	    set @SQL1 += left(@source_column_list, len(@source_column_list) -1) + @crlf + 
		          'FROM ' + quotename(@dv_timevault_name) + '.'+quotename(@vault_source_schema)+ '.'+quotename(@vault_source_table) + @crlf +
				  'UNION ' + @crlf  
END   
set @SQL1 = left(@SQL1, len(@SQL1) -10)  + ')' + @crlf +              
			'MERGE ' + quotename(@hub_database) +'.'+quotename(@hub_schema)+'.'+ quotename(@hub_table_name) + ' WITH (HOLDLOCK) AS TARGET ' + @crlf +
            'USING wBaseSet AS SOURCE' + @crlf + ' ON ' + left(@match_list, len(@match_list) - 4) + @crlf +
			'WHEN NOT MATCHED BY TARGET THEN ' + @crlf + 'INSERT(' + @dv_load_date_time_column + ',' + @dv_data_source_column + ',' + replace(left(@hub_column_list, len(@hub_column_list) -1), 'SOURCE.','') + ')' + @crlf +
			'VALUES(sysdatetimeoffset(),''' + cast(@dv_data_source_key as varchar(50)) + ''',' + left(@hub_column_list, len(@hub_column_list) -1) + ')' + @crlf +
			'OUTPUT $action into @rowcounts;' + @crlf + 'select @insertcount = count(*) from @rowcounts;'
CLOSE cur_hub_column   
DEALLOCATE cur_hub_column


/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Load The Hub'
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @SQL1 + @crlf
SET @ParmDefinition = N'@insertcount int OUTPUT';

EXECUTE sp_executesql @SQL1, @ParmDefinition, @insertcount = @hub_insert_count OUTPUT;

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
--print @_ProgressText
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