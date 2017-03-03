


CREATE PROCEDURE [dv_integrity].[dv_build_match_script]
(
  @left_object_name				nvarchar(128)     = NULL
, @left_object_schema			nvarchar(128)     = NULL
, @left_object_database			nvarchar(128)     = NULL
, @left_object_type				varchar(50)		  = NULL
, @left_sat_pit					datetimeoffset(7) = NULL
, @left_object_filter			nvarchar(4000)	  = NULL  -- NB - Not used yet.
, @right_object_name			nvarchar(128)     = NULL
, @right_object_schema			nvarchar(128)     = NULL
, @right_object_database		nvarchar(128)     = NULL
, @right_object_type			varchar(50)		  = NULL
, @right_sat_pit				datetimeoffset(7) = NULL
, @right_object_filter			nvarchar(4000)	  = NULL  -- NB - Not Used Yet.
, @output_database				nvarchar(128)     = NULL
, @output_schema				nvarchar(128)     = NULL
, @output_name					nvarchar(128)     = NULL
, @select_into					bit               = 0
, @match_key					int               = NULL
, @payload_columns			    [dbo].[dv_column_matching_list] READONLY
, @vault_sql_statement          nvarchar(max) OUTPUT
, @dogenerateerror              bit				  = 0
, @dothrowerror                 bit				  = 1
)
AS
BEGIN
SET NOCOUNT ON
--Defaults

DECLARE @crlf char(2) = CHAR(13) + CHAR(10)
DECLARE  
-- Hub Defaults									
		 @def_hub_schema				varchar(128)
--Link Defaults									
		,@def_link_schema				varchar(128)
--Sat Defaults									
		,@def_sat_schema				varchar(128)
-- Source Defaults									
		,@def_stg_schema				varchar(128)
--Working Storage
DECLARE  					
         @left_object_qualified_name	varchar(512)
		,@right_object_qualified_name	varchar(512)
		,@output_object_qualified_name  varchar(512)
		,@sql							nvarchar(max)
		,@sqlLeft						nvarchar(max)
		,@sqlRight						nvarchar(max)
		,@left_object_config_key		int
		,@right_object_config_key		int
		,@output_object_config_key		int
		,@stage_Load_Date_Time_column	varchar(128)
		,@stage_Source_Version_Key_column varchar(128)
		,@stage_match_key_column        varchar(128)
		,@stage_master_table_column		varchar(128)
		,@stage_column_list             varchar(max)
DECLARE @leftColumnList table(ColumnSQL varchar(512))			
DECLARE @payload_columns_ordered table([left_column_name] varchar(128), [right_column_name] varchar(128), [column_order] int identity(1,1))
INSERT  @payload_columns_ordered select * from @payload_columns order by 1, 2
-- Log4TSQL Journal Constants 										
DECLARE @SEVERITY_CRITICAL				smallint = 1;
DECLARE @SEVERITY_SEVERE				smallint = 2;
DECLARE @SEVERITY_MAJOR					smallint = 4;
DECLARE @SEVERITY_MODERATE				smallint = 8;
DECLARE @SEVERITY_MINOR					smallint = 16;
DECLARE @SEVERITY_CONCURRENCY			smallint = 32;
DECLARE @SEVERITY_INFORMATION			smallint = 256;
DECLARE @SEVERITY_SUCCESS				smallint = 512;
DECLARE @SEVERITY_DEBUG					smallint = 1024;
DECLARE @NEW_LINE						char(1)  = CHAR(10);

-- Log4TSQL Standard/ExceptionHandler variables
DECLARE	  @_Error						int
		, @_RowCount					int
		, @_Step						varchar(128)
		, @_Message						nvarchar(512)
		, @_ErrorContext				nvarchar(512)
-- Log4TSQL JournalWriter variables
DECLARE   @_FunctionName				varchar(255)
		, @_SprocStartTime				datetime
		, @_JournalOnOff				varchar(3)
		, @_Severity					smallint
		, @_ExceptionId					int
		, @_StepStartTime				datetime
		, @_ProgressText				nvarchar(max)
SET @_Error             = 0;
SET @_FunctionName      = OBJECT_NAME(@@PROCID);
SET @_Severity          = @SEVERITY_INFORMATION;
SET @_SprocStartTime    = sysdatetimeoffset();
SET @_ProgressText      = '' 
SET @_JournalOnOff      = log4.GetJournalControl(@_FunctionName, 'HOWTO');  -- left Group Name as HOWTO for now.


-- Get Defaults	from ODE Config						
set @sql = ''
select @sql += '[' + left_column_name + ' , ' + right_column_name + ']'
from @payload_columns

-- set the Parameters for logging:

SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @left_object_name             : ' + COALESCE(@left_object_name		, '<NULL>')
						+ @NEW_LINE + '    @left_object_schema           : ' + COALESCE(@left_object_schema		, '<NULL>')
						+ @NEW_LINE + '    @left_object_database         : ' + COALESCE(@left_object_database	, '<NULL>')
						+ @NEW_LINE + '    @left_object_type             : ' + COALESCE(@left_object_type		, '<NULL>')
						+ @NEW_LINE + '    @left_object_filter           : ' + COALESCE(@left_object_filter		, '<NULL>')
						+ @NEW_LINE + '    @right_object_name            : ' + COALESCE(@right_object_name		, '<NULL>')
						+ @NEW_LINE + '    @right_object_schema          : ' + COALESCE(@right_object_schema	, '<NULL>')
						+ @NEW_LINE + '    @right_object_database        : ' + COALESCE(@right_object_database  , '<NULL>')
						+ @NEW_LINE + '    @right_object_type            : ' + COALESCE(@right_object_type		, '<NULL>')
						+ @NEW_LINE + '    @right_object_filter          : ' + COALESCE(@right_object_filter	, '<NULL>')
						+ @NEW_LINE + '    @payload_columns              : ' + COALESCE(@sql					, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar)   , '<NULL>')
						+ @NEW_LINE

--print @_ProgressText
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

if      @left_object_type = 'hub' select @left_object_config_key = [hub_key]			from [dbo].[dv_hub]			where [hub_database] = @left_object_database		and [hub_schema]		= @left_object_schema	and [hub_name] = @left_object_name
else if @left_object_type = 'lnk' select @left_object_config_key = [link_key]			from [dbo].[dv_link]		where [link_database] = @left_object_database		and [link_schema]		= @left_object_schema	and [link_name] = @left_object_name
else if @left_object_type = 'sat' select @left_object_config_key = [satellite_key]		from [dbo].[dv_satellite]	where [satellite_database] = @left_object_database	and [satellite_schema]	= @left_object_schema	and [satellite_name] = @left_object_name
else if @left_object_type = 'stg' select @left_object_config_key = [source_table_key]	
			from [dbo].[dv_source_table] st
			inner join [dbo].[dv_stage_schema] sc	on sc.stage_schema_key = st.stage_schema_key
			inner join [dbo].[dv_stage_database] sd on sd.stage_database_key = sc.stage_database_key
			where sd.[stage_database_name]	= @left_object_database 
			and sc.[stage_schema_name]		= @left_object_schema	
			and st.[stage_table_name]		= @left_object_name
else RAISERROR('%s is not a valid Object type', 16, 1,@left_object_type)

select @stage_Load_Date_Time_column		= [column_name]
from [dbo].[dv_default_column]
where object_column_type = 'Load_Date_Time'
and object_type = 'stg'
select @stage_Source_Version_Key_column	= [column_name]
from [dbo].[dv_default_column]
where object_column_type = 'Source_Version_Key'	
and object_type = 'stg'

select @stage_match_key_column	= [column_name]
from [dbo].[dv_default_column]
where object_column_type = 'MatchKeyColumn'	
and object_type = 'mtc'
select @stage_master_table_column	= [column_name]
from [dbo].[dv_default_column]
where object_column_type = 'MasterTableColumn'	
and object_type = 'mtc'

if      @right_object_type = 'hub' select @right_object_config_key = [hub_key]			from [dbo].[dv_hub]			where [hub_database] = @right_object_database		and [hub_schema]		= @right_object_schema	and [hub_name] = @right_object_name
else if @right_object_type = 'lnk' select @right_object_config_key = [link_key]			from [dbo].[dv_link]		where [link_database] = @right_object_database		and [link_schema]		= @right_object_schema	and [link_name] = @right_object_name
else if @right_object_type = 'sat' select @right_object_config_key = [satellite_key]	from [dbo].[dv_satellite]	where [satellite_database] = @right_object_database	and [satellite_schema]	= @right_object_schema	and [satellite_name] = @right_object_name
else if @right_object_type = 'stg' select @right_object_config_key = [source_table_key]	
			from [dbo].[dv_source_table] st
			inner join [dbo].[dv_stage_schema] sc	on sc.stage_schema_key = st.stage_schema_key
			inner join [dbo].[dv_stage_database] sd on sd.stage_database_key = sc.stage_database_key
			where sd.[stage_database_name]	= @right_object_database 
			and sc.[stage_schema_name]		= @right_object_schema	
			and st.[stage_table_name]		= @right_object_name

else RAISERROR('%s is not a valid Object type', 16, 1,@right_object_type)

if @select_into = 0 and isnull(@output_name, '') <> ''
	begin
	select @output_object_config_key = [source_table_key]	
			from [dbo].[dv_source_table] st
			inner join [dbo].[dv_stage_schema] sc	on sc.stage_schema_key = st.stage_schema_key
			inner join [dbo].[dv_stage_database] sd on sd.stage_database_key = sc.stage_database_key
			where sd.[stage_database_name]	= @output_database 
			and sc.[stage_schema_name]		= @output_schema	
			and st.[stage_table_name]		= @output_name
	if @output_object_config_key is null RAISERROR('%s.%s.%s is not a valid Stage Table', 16, 1,@output_database, @output_schema, @output_name)
	end
/*--------------------------------------------------------------------------------------------------------------*/
-- Get Defaults	from ODE Config
select
 @def_hub_schema        = cast([dbo].[fn_get_default_value] ('schema','hub')			as varchar(128))
,@def_link_schema       = cast([dbo].[fn_get_default_value] ('schema','lnk')			as varchar(128))
,@def_sat_schema        = cast([dbo].[fn_get_default_value] ('schema','sat')			as varchar(128))
,@def_stg_schema		= cast([dbo].[fn_get_default_value] ('schema','stg')			as varchar(128))


set @left_object_qualified_name = case 
    when @left_object_type = 'sat'  then quotename(@left_object_database) + '.' + quotename(coalesce(@left_object_schema, @def_sat_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (@left_object_name, 'sat'))+ '_left')
	when @left_object_type = 'lnk'  then quotename(@left_object_database) + '.' + quotename(coalesce(@left_object_schema, @def_link_schema,'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (@left_object_name, 'lnk'))+ '_left')
	when @left_object_type = 'hub'  then quotename(@left_object_database) + '.' + quotename(coalesce(@left_object_schema, @def_hub_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (@left_object_name, 'hub'))+ '_left')
	when @left_object_type = 'stg'  then quotename(@left_object_database) + '.' + quotename(coalesce(@left_object_schema, @def_stg_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (@left_object_name, 'stg'))+ '_left')
	else 'Unknown'
	end;

if @left_object_qualified_name = 'Unknown' RAISERROR('%s is not a valid Object type', 16, 1,@left_object_type)

set @right_object_qualified_name = case 
    when @right_object_type = 'sat'  then quotename(@right_object_database) + '.' + quotename(coalesce(@right_object_schema, @def_sat_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (@right_object_name, 'sat'))+ '_right')
	when @right_object_type = 'lnk'  then quotename(@right_object_database) + '.' + quotename(coalesce(@right_object_schema, @def_link_schema,'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (@right_object_name, 'lnk'))+ '_right')
	when @right_object_type = 'hub'  then quotename(@right_object_database) + '.' + quotename(coalesce(@right_object_schema, @def_hub_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (@right_object_name, 'hub'))+ '_right')
	when @right_object_type = 'stg'  then quotename(@right_object_database) + '.' + quotename(coalesce(@right_object_schema, @def_hub_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (@right_object_name, 'stg'))+ '_right')
	else 'Unknown'
	end;
if @right_object_qualified_name = 'Unknown' RAISERROR('%s is not a valid Object type', 16, 1,@right_object_type)

set @output_object_qualified_name = case when isnull(@output_database, '') = '' then '' else quotename(@output_database) + '.' end +
									case when isnull(@output_schema, '') = ''   then '' else quotename(@output_schema) + '.'   end +
                                    quotename(@output_name)
set @stage_column_list = '('
select @stage_column_list += column_name + ', '
FROM [dbo].[vw_stage_table] st
  inner join [dbo].[dv_column] c
  on st.source_table_key = c.table_key
  where 1=1
  and st.stage_database		=  @output_database	
  and st.stage_schema		= @output_schema
  and st.stage_table_name	= @output_name	
  order by column_name
set @stage_column_list = left(@stage_column_list, len(@stage_column_list) - 1) + ')'

select @sqlLeft = 'SELECT ' 
if @left_object_type = 'lnk'
begin 
select @sqlLeft += column_qualified_name + ' AS ' + quotename(lkc.link_key_column_name + '__' + [column_name]) + ',' + @crlf
from [dbo].[dv_link] l
	inner join [dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
	inner join [dbo].[dv_hub_column] hc on hc.link_key_column_key = lkc.link_key_column_key
	inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key_column_key = hc.hub_key_column_key
	inner join [dbo].[dv_hub] h on h.hub_key = hkc.hub_key
	inner join @payload_columns_ordered pc on hkc.[hub_key_column_name] = pc.[left_column_name]
	cross apply [dbo].[fn_get_object_column_list](h.hub_key, 'hub', 'hub_' + lkc.link_key_column_name)
	where l.link_key = @left_object_config_key
	order by pc.column_order
set @sqlLeft = left(@sqlLeft, len(@sqlLeft) -3) + @crlf 
select @sqlLeft +=[dbo].[fn_get_object_from_statement](@left_object_config_key, @left_object_type, DEFAULT) + @crlf
select @sqlLeft +=[dbo].[fn_get_object_join_statement] (@left_object_config_key, @left_object_type, DEFAULT, h.Hub_key, 'hub', 'hub_' + lkc.link_key_column_name) + @crlf
from [dbo].[dv_link] l
	inner join [dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
	inner join [dbo].[dv_hub_column] hc on hc.link_key_column_key = lkc.link_key_column_key
	inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key_column_key = hc.hub_key_column_key
	inner join [dbo].[dv_hub] h on h.hub_key = hkc.hub_key
	where l.link_key = @left_object_config_key 
select @sqlLeft += 'WHERE 1=1 ' 
end
else 
begin
select @sqlLeft += l.[column_qualified_name] + ',' + @crlf
from @payload_columns_ordered pc
inner join [dbo].[fn_get_object_column_list] (@left_object_config_key, @left_object_type, DEFAULT) l on l.[column_name] = pc.[left_column_name]
order by pc.column_order
set @sqlLeft = left(@sqlLeft, len(@sqlLeft) -3)  + @crlf
select @sqlLeft +=[dbo].[fn_get_object_from_statement](@left_object_config_key, @left_object_type, DEFAULT) + @crlf + 'WHERE 1=1' 
if @left_object_type = 'sat'
	begin 
	select @sqlLeft += ' AND ' + [dbo].[fn_get_satellite_pit statement](@left_sat_pit)
	end

end

select @sqlRight = 'SELECT ' + @crlf
if @right_object_type = 'lnk'
begin 
select @sqlRight += column_qualified_name + ' AS ' + quotename(lkc.link_key_column_name + '__' + [column_name]) + ',' + @crlf
from [dbo].[dv_link] l
	inner join [dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
	inner join [dbo].[dv_hub_column] hc on hc.link_key_column_key = lkc.link_key_column_key
	inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key_column_key = hc.hub_key_column_key
	inner join [dbo].[dv_hub] h on h.hub_key = hkc.hub_key
	inner join @payload_columns_ordered pc on hkc.[hub_key_column_name] = pc.[left_column_name]
	cross apply [dbo].[fn_get_object_column_list](h.hub_key, 'hub', 'hub_' + lkc.link_key_column_name)
	where l.link_key = @right_object_config_key 
	order by pc.column_order
set @sqlRight = left(@sqlRight, len(@sqlRight) -3) + @crlf 
select @sqlRight +=[dbo].[fn_get_object_from_statement](@right_object_config_key, @right_object_type, DEFAULT) + @crlf
select @sqlRight +=[dbo].[fn_get_object_join_statement] (@right_object_config_key, @right_object_type, DEFAULT, h.Hub_key, 'hub', 'hub_' + lkc.link_key_column_name) + @crlf
from [dbo].[dv_link] l
	inner join [dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
	inner join [dbo].[dv_hub_column] hc on hc.link_key_column_key = lkc.link_key_column_key
	inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key_column_key = hc.hub_key_column_key
	inner join [dbo].[dv_hub] h on h.hub_key = hkc.hub_key
	where l.link_key = @right_object_config_key 
select @sqlRight += 'WHERE 1=1 ' 
end
else 
begin
select @sqlRight += case 
				when r.[column_type] <> l.[column_type] then r.[column_qualified_name]
				else 'CAST(' + r.[column_qualified_name] + ' AS ' + rtrim(l.[column_definition]) + ') AS ' + r.column_name
				end + ',' + @crlf
from @payload_columns_ordered pc
inner join [dbo].[fn_get_object_column_list] (@left_object_config_key, @left_object_type, DEFAULT) l on l.[column_name] = pc.[left_column_name]
inner join [dbo].[fn_get_object_column_list] (@right_object_config_key, @right_object_type, DEFAULT) r on r.[column_name] = pc.[right_column_name] 
order by pc.column_order
set @sqlRight = left(@sqlRight, len(@sqlRight) -3)  + @crlf
select @sqlRight +=[dbo].[fn_get_object_from_statement](@right_object_config_key, @right_object_type, DEFAULT) + @crlf + 'WHERE 1=1' 
if @right_object_type = 'sat' 
	begin
	select @sqlRight += ' AND ' + [dbo].[fn_get_satellite_pit statement](@right_sat_pit)
	end

end
select @sql = ';WITH wLeft as (' + @crlf +
			  @sqlLeft + @crlf +
			  'EXCEPT' + @crlf +
			  @sqlRight + @crlf + ')' + @crlf +
			  ',wRight as (' + @crlf +
			  @sqlRight + @crlf +
			  'EXCEPT' + @crlf +
			  @sqlLeft + @crlf + ')' + @crlf +
			  ',wMatch as (' + @crlf +
			  'SELECT ''' + @left_object_qualified_name + ''' AS [' + @stage_master_table_column + '], * FROM wLeft' + @crlf +
			  'UNION ALL' + @crlf +
			  'SELECT ''' + @right_object_qualified_name + ''' AS [' + @stage_master_table_column + '], * FROM wRight)' + @crlf 

if @select_into = 0 and isnull(@output_name, '') <> '' select @sql += @crlf + 'INSERT ' + @output_object_qualified_name + ' ' + @stage_column_list + @crlf

-- Build the INSERT Column List:
insert @leftColumnList(ColumnSQL) select quotename(left_column_name) from @payload_columns
insert @leftColumnList(ColumnSQL) select quotename(@stage_Load_Date_Time_column) + ' = sysdatetimeoffset()'
insert @leftColumnList(ColumnSQL) select quotename(@stage_master_table_column)
if isnull(@match_key, '') <> '' 
	insert @leftColumnList(ColumnSQL) select quotename(@stage_Source_Version_Key_column) + ' = ' + cast(@match_key as varchar(50))
insert @leftColumnList(ColumnSQL) select quotename(@stage_match_key_column) + ' = row_number() over (order by ' + quotename(@stage_master_table_column) + ')'

select @sql += @crlf + 'SELECT '
select @sql += ColumnSQL + ', ' 
from @leftColumnList
order by ColumnSQL
select @sql = left(@sql, len(@sql) - 1)
if @select_into = 1 and isnull(@output_name, '') <> '' select @sql += @crlf + ' INTO ' + @output_object_qualified_name + @crlf
select @sql += ' FROM wMatch'
/*--------------------------------------------------------------------------------------------------------------*/
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @SQL + @crlf
--print @SQL --**************
set @vault_sql_statement = @sql 
/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created Compare Statement' 

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create Compare Statement: ' 
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