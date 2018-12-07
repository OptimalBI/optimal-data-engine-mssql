CREATE PROCEDURE [dbo].[dv_load_link_table]
(
  @vault_source_unique_name             varchar(256)    = NULL
, @vault_link_name                      varchar(256)    = NULL
, @vault_source_version_key				int				= NULL
, @vault_runkey							int				= NULL
, @dogenerateerror                      bit             = 0
, @dothrowerror                         bit             = 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly
--declare @sat_name varchar(100) =  'AdventureWorks2014_production_productinventory'

-- System Wide Defaults
-- Local Defaults Values
DECLARE @crlf	char(2)	= CHAR(13) + CHAR(10)
-- Global Defaults
DECLARE
         @def_global_lowdate									datetime
		,@def_global_highdate									datetime
        ,@def_global_default_load_date_time						varchar(128)
        ,@dv_load_date_time_column								varchar(128)
        ,@def_global_failed_lookup_key							int
-- Hub Defaults
        ,@def_hub_prefix                                        varchar(128)
        ,@def_hub_schema                                        varchar(128)
        ,@def_hub_filegroup                                     varchar(128)
--Link Defaults
        ,@def_link_prefix                                       varchar(128)
        ,@def_link_schema                                       varchar(128)
        ,@def_link_filegroup									varchar(128)
--Sat Defaults
        ,@def_sat_prefix                                        varchar(128)
        ,@def_sat_schema                                        varchar(128)
        ,@def_sat_filegroup                                     varchar(128)
        ,@sat_start_date_col									varchar(128)
        ,@sat_end_date_col                                      varchar(128)

-- Object Specific Settings
-- Stage Table
--,@source_system                                         varchar(128)
        ,@stage_database                                        varchar(128)
        ,@stage_schema                                          varchar(128)
        ,@stage_table                                           varchar(128)
        ,@stage_table_config_key								int
		,@stage_source_version_key								int
        ,@stage_qualified_name									varchar(512)
        ,@stage_load_date_time									varchar(128)
        ,@stage_payload                                         varchar(max)
-- Link Table
        ,@link_database                                         varchar(128)
        ,@link_schema                                           varchar(128)
        ,@link_table                                            varchar(128)
        ,@link_surrogate_keyname								varchar(128)
        ,@link_config_key                                       int
        ,@link_qualified_name									varchar(512)
        ,@link_technical_columns								nvarchar(max)
        ,@link_hub_keys                                         nvarchar(max)

--  Working Storage
DECLARE @sat_insert_count									int
       ,@temp_table_name									varchar(116)
       ,@sql                                                nvarchar(max)
       ,@sql1                                               nvarchar(max)
       ,@sql2                                               nvarchar(max)
       ,@surrogate_key_match								nvarchar(max)
DECLARE @declare											nvarchar(512)   = ''
DECLARE @count_rows											nvarchar(256)   = ''
DECLARE @match_list											nvarchar(max)   = ''
DECLARE @value_list											nvarchar(max)   = ''
DECLARE @sat_column_list									nvarchar(max)   = ''
DECLARE @hub_column_list									nvarchar(max)   = ''

DECLARE @ParmDefinition										nvarchar(500);
DECLARE @insert_count										int;
DECLARE @rc													int;

DECLARE @wrk_hub_joins										varchar(max)
DECLARE @wrk_link_keys										varchar(max)
DECLARE @wrk_link_match										varchar(max)
-- Log4TSQL Journal Constants
DECLARE @SEVERITY_CRITICAL									smallint = 1;
DECLARE @SEVERITY_SEVERE									smallint = 2;
DECLARE @SEVERITY_MAJOR										smallint = 4;
DECLARE @SEVERITY_MODERATE									smallint = 8;
DECLARE @SEVERITY_MINOR										smallint = 16;
DECLARE @SEVERITY_CONCURRENCY								smallint = 32;
DECLARE @SEVERITY_INFORMATION								smallint = 256;
DECLARE @SEVERITY_SUCCESS									smallint = 512;
DECLARE @SEVERITY_DEBUG										smallint = 1024;
DECLARE @NEW_LINE											char(1)  = CHAR(10);

-- Log4TSQL Standard/ExceptionHandler variables
DECLARE   @_Error											int
        , @_RowCount										int
        , @_Step											varchar(128)
        , @_Message											nvarchar(512)
        , @_ErrorContext									nvarchar(512)

-- Log4TSQL JournalWriter variables
DECLARE   @_FunctionName									varchar(255)
        , @_SprocStartTime									datetime
        , @_JournalOnOff									varchar(3)
        , @_Severity										smallint
        , @_ExceptionId										int
        , @_StepStartTime									datetime
        , @_ProgressText									nvarchar(max)

SET @_Error             = 0;
SET @_FunctionName      = OBJECT_NAME(@@PROCID);
SET @_Severity          = @SEVERITY_INFORMATION;
SET @_SprocStartTime    = sysdatetimeoffset();
SET @_ProgressText      = ''
SET @_JournalOnOff      = log4.GetJournalControl(@_FunctionName, 'HOWTO');  -- left Group Name as HOWTO for now.


-- set Log4TSQL Parameters for Logging:
SET @_ProgressText              = @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
                                                + @NEW_LINE + '    @vault_source_unique_name     : ' + COALESCE(@vault_source_unique_name, 'NULL')
												+ @NEW_LINE + '    @vault_link_name              : ' + COALESCE(@vault_link_name, 'NULL')
                                                + @NEW_LINE + '    @vault_runkey                 : ' + COALESCE(CAST(@vault_runkey AS varchar), 'NULL')
                                                + @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
                                                + @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
                                                + @NEW_LINE
BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';
IF ((@vault_runkey is not null) and ((select count(*) from [dv_scheduler].[dv_run] where @vault_runkey = [run_key]) <> 1))
			RAISERROR('Invalid @vault_runkey provided: %i', 16, 1, @vault_runkey);

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'

-- System Wide Defaults
select
-- Global Defaults
 @def_global_lowdate                            = cast([dbo].[fn_get_default_value] ('LowDate','Global')							as datetime)
,@def_global_highdate                           = cast([dbo].[fn_get_default_value] ('HighDate','Global')							as datetime)
,@def_global_default_load_date_time				= cast([dbo].[fn_get_default_value] ('DefaultLoadDateTime','Global')				as varchar(128))
,@def_global_failed_lookup_key					= cast([dbo].[fn_get_default_value] ('FailedLookupKey', 'Global')					as integer)

-- Hub Defaults
,@def_hub_prefix                                = cast([dbo].[fn_get_default_value] ('prefix','hub')                                as varchar(128))
,@def_hub_schema                                = cast([dbo].[fn_get_default_value] ('schema','hub')                                as varchar(128))
,@def_hub_filegroup                             = cast([dbo].[fn_get_default_value] ('filegroup','hub')								as varchar(128))
-- Link Defaults
,@def_link_prefix                               = cast([dbo].[fn_get_default_value] ('prefix','lnk')                                as varchar(128))
,@def_link_schema                               = cast([dbo].[fn_get_default_value] ('schema','lnk')                                as varchar(128))
,@def_link_filegroup							= cast([dbo].[fn_get_default_value] ('filegroup','lnk')								as varchar(128))
-- Sat Defaults
,@def_sat_prefix                                = cast([dbo].[fn_get_default_value] ('prefix','sat')                                as varchar(128))
,@def_sat_schema                                = cast([dbo].[fn_get_default_value] ('schema','sat')                                as varchar(128))
,@def_sat_filegroup                             = cast([dbo].[fn_get_default_value] ('filegroup','sat')								as varchar(128))

-- Object Specific Settings
-- Source Table
select   @stage_database                        = sdb.[stage_database_name]
        ,@stage_schema                          = ss.[stage_schema_name]
        ,@stage_table                           = st.[stage_table_name]
        ,@stage_table_config_key				= st.[source_table_key]
		,@stage_source_version_key				= isnull(@vault_source_version_key, sv.source_version_key) -- if no source version is provided, use the current source version for the source table used as source for this load.
        ,@stage_qualified_name					= quotename(sdb.[stage_database_name]) + '.' + quotename(ss.[stage_schema_name]) + '.' + quotename(st.[stage_table_name])
from [dbo].[dv_source_table] st
inner join [dbo].[dv_stage_schema] ss on ss.stage_schema_key = st.stage_schema_key
inner join [dbo].[dv_stage_database] sdb on sdb.stage_database_key = ss.stage_database_key
left join  [dbo].[dv_source_version] sv on sv.source_table_key = st.source_table_key	
									   and sv.is_current= 1
where 1=1
and st.[source_unique_name]						= @vault_source_unique_name

if @@ROWCOUNT <> 1 RAISERROR ('Invalid Link Parameters Supplied',16,1);
select @rc = count(*) from [dbo].[dv_source_version] where source_version_key = @stage_source_version_key and is_current= 1
if @rc <> 1 RAISERROR('dv_source_table or current dv_source_version missing for: %s, source version : %i', 16, 1, @stage_qualified_name, @stage_source_version_key);

begin
        select   @link_database                 = l.[link_database]
                ,@link_schema                   = coalesce(l.[link_schema], @def_link_schema, 'dbo')
                ,@link_table                    = l.[link_name]
				,@link_surrogate_keyname		= (select column_name from [dbo].[fn_get_key_definition]([link_name], 'lnk'))
                ,@link_config_key               = l.[link_key]
                ,@link_qualified_name			= quotename([link_database]) + '.' + quotename(coalesce(l.[link_schema], @def_link_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([link_name], 'lnk')))
		from [dbo].[dv_link] l
		inner join [dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
		inner join [dbo].[dv_hub_column] hc on hc.link_key_column_key = lkc.link_key_column_key
		inner join [dbo].[dv_column] c on c.column_key = hc.column_key
		inner join [dbo].[dv_source_table] st on st.source_table_key = c.table_key
		where 1=1
		and st.source_unique_name= @vault_source_unique_name
		and l.[link_name] = @vault_link_name

--set @link_hub_keys = ''

declare  @c_hub_key							int
		,@c_hub_name                        varchar(128)
        ,@c_hub_schema						varchar(128)
        ,@c_hub_database					varchar(128)
		,@c_link_key_name					varchar(128)
		,@c_link_key_column_key				int

set @link_hub_keys	= ''
set @wrk_link_keys	= ''
set @wrk_link_match = '' 
set @wrk_hub_joins	= ''

DECLARE c_hub_key CURSOR FOR
select distinct h.[hub_key]
      ,h.[hub_name]
      ,h.[hub_schema]
      ,h.[hub_database]
	  ,[link_key_name] = isnull(lkc.[link_key_column_name],h.[hub_name])
	  ,lkc.link_key_column_key 
FROM [dbo].[dv_link] l
inner join [dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
inner join [dbo].[dv_hub_column] hc on hc.link_key_column_key = lkc.link_key_column_key
inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key_column_key = hc.hub_key_column_key
inner join [dbo].[dv_hub] h on h.hub_key = hkc.hub_key
inner join [dbo].[dv_column] c on c.column_key = hc.column_key
inner join [dbo].[dv_source_table] st on st.source_table_key = c.table_key
where 1=1
  and l.[link_key] = @link_config_key
  and st.source_table_key = @stage_table_config_key
  and c.is_retired <> 1
order by h.[hub_key]

OPEN c_hub_key

FETCH NEXT FROM c_hub_key
INTO @c_hub_key
    ,@c_hub_name
    ,@c_hub_schema
    ,@c_hub_database
	,@c_link_key_name
	,@c_link_key_column_key

WHILE @@FETCH_STATUS = 0
BEGIN
        select  @wrk_hub_joins   += (select column_name from [dbo].[fn_get_key_definition](@c_link_key_name, 'hub')) + ', '
			   ,@wrk_link_keys   += ' tmp.' + (select column_name from [dbo].[fn_get_key_definition](@c_link_key_name, 'hub')) + ' = link.' + (select column_name from [dbo].[fn_get_key_definition](@c_link_key_name, 'hub')) + @crlf + ' AND '
			   ,@wrk_link_match  += (select column_name from [dbo].[fn_get_key_definition](@c_link_key_name, 'hub'))  + ' , '

         from (
        select distinct
            h.hub_name
        from [dbo].[dv_hub] h
        inner join [dbo].[dv_hub_key_column] hkc
        on h.hub_key = hkc.hub_key
        inner join [dbo].[dv_hub_column] hc
        on hc.hub_key_column_key = hkc.hub_key_column_key
        inner join [dbo].[dv_column] c
        on c.column_key = hc.column_key
        inner join [dbo].[dv_source_table] st
        on c.[table_key] = st.[source_table_key]
        where 1=1
        and h.hub_key = @c_hub_key
        and st.[source_table_key] = @stage_table_config_key
        and c.is_retired <> 1) hkc
        set @link_hub_keys = @link_hub_keys + @wrk_link_keys
        FETCH NEXT FROM c_hub_key
        INTO @c_hub_key
                ,@c_hub_name
                ,@c_hub_schema
                ,@c_hub_database
				,@c_link_key_name
				,@c_link_key_column_key
END

CLOSE c_hub_key
DEALLOCATE c_hub_key
select @wrk_link_keys	= left(@wrk_link_keys, len(@wrk_link_keys) - 4)
select @wrk_hub_joins	= left(@wrk_hub_joins, len(@wrk_hub_joins) - 1)
select @wrk_link_match	= left(@wrk_link_match, len(@wrk_link_match) -2)
end

---- Use either a date time from the source or the default
select @stage_load_date_time = [column_name]
from [dbo].[dv_source_table] st
inner join [dbo].[dv_column] c
on st.[source_table_key] = c.table_key
where 1=1
and st.[source_table_key] = @stage_table_config_key
and c.[is_source_date] = 1
and c.[is_retired] <> 1
if @@rowcount > 1 RAISERROR ('Source Table has Multiple Source Dates Defined',16,1);
select @stage_load_date_time = isnull(@stage_load_date_time, @def_global_default_load_date_time)

-- Build the Source Payload NB - needs to join to the Sat Table to get each satellite related to the source.
set @sql = ''
select @sql += 'src.' +quotename([column_name]) + @crlf +', '
from [dbo].[dv_column]
where 1=1
and [is_retired] <> 1
and [table_key] = @stage_table_config_key
order by source_ordinal_position
select @stage_payload = left(@sql, len(@sql) -1)

---- Build the Link Payload
set @sql = ''
select @sql += quotename([column_name]) +', '
from [dbo].[dv_default_column]
where 1=1
and object_column_type <> 'Object_Key'
and [object_type] = 'Lnk'
order by [ordinal_position]
set @link_technical_columns = @sql

-- Compile the SQL
--SQL to do the look up the hub keys that make up the link

EXECUTE [dbo].[dv_load_source_table_key_lookup] @vault_source_unique_name, 'Y', 'Full', @temp_table_name OUTPUT, @sql OUTPUT  -- Note, load type is set to "Full". This is a placeholder. Link processing does not use Run Types.
set @sql1 = @sql
--select @sql1 += [dv_scripting].[fn_get_task_log_insert_statement] ('', '', '', 0) -- add the logging variables.
set @sql1 += 'DECLARE @rowcounts TABLE(merge_action nvarchar(10));' + @crlf
set @sql1 += 'SELECT @version_date = sysdatetimeoffset()' + @crlf 
set @sql1 += 'SELECT @__load_start_date = sysdatetimeoffset()' + @crlf 
set @sql1 += 'SELECT @__vault_runkey = ' + isnull(cast(@vault_runkey as varchar(20)), '0') + @crlf 
--set @sql1 += 'DECLARE @load_end_datetime datetimeoffset(7)' + @crlf 
set @sql1 += 'BEGIN TRANSACTION' + @crlf 
set @sql1 += ';WITH wBaseSet AS (SELECT ' + @wrk_link_match + ' FROM ' + quotename(@temp_table_name) + ')' + @crlf
set @sql1 += 'MERGE ' + @link_qualified_name + ' WITH (HOLDLOCK) AS link' + @crlf
set @sql1 += 'USING wBaseSet AS tmp' + @crlf
set @sql1 += 'ON' + @wrk_link_keys
set @sql1 += 'WHEN NOT MATCHED BY TARGET THEN ' + @crlf
set @sql1 += 'INSERT(' + @link_technical_columns + @wrk_hub_joins +  ')' + @crlf
set @sql1 += 'VALUES(@version_date, ''' + cast(@stage_source_version_key as varchar(20)) + ''',' + @wrk_hub_joins + ')OUTPUT $action into @rowcounts;' + @crlf
set @sql1 += 'SELECT @__rows_inserted = count(*) from @rowcounts;' + @crlf
set @sql1 += 'SELECT @__load_end_date = sysdatetimeoffset();' + @crlf
set @sql1 += 'SET @__high_water_date = @version_date;' + @crlf
-- Log Completion
select @sql1 += [dv_scripting].[fn_get_task_log_insert_statement] (@stage_source_version_key, 'link', @link_config_key, 0)
set @sql1 += 'COMMIT;' + @crlf
set @sql = @sql1

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Load The Link'
IF @_JournalOnOff = 'ON'
        SET @_ProgressText += @SQL
SET @ParmDefinition = N'@insertcount int OUTPUT';
--print @SQL
EXECUTE sp_executesql @SQL, @ParmDefinition, @insertcount = @insert_count OUTPUT;

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
                                + 'Step: [' + @_Step + '] completed '

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Loaded Object: ' + @link_qualified_name

END TRY
BEGIN CATCH
SET @_ErrorContext      = 'Failed to Load Object: ' + @link_qualified_name


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
                        SET @_Step                      = 'OnComplete'
                        SET @_Severity          = @SEVERITY_SUCCESS
                        SET @_Message           = COALESCE(@_Message, @_Step)
                                                                + ' in a total run time of ' + log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
                        SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
                END
        ELSE
                BEGIN
                        SET @_Step                      = COALESCE(@_Step, 'OnError')
                        SET @_Severity          = @SEVERITY_SEVERE
                        SET @_Message           = COALESCE(@_Message, @_Step)
                                                                + ' after a total run time of ' + log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
                        SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
                END
--print @_ProgressText 
        IF @_JournalOnOff = 'ON'
                EXEC log4.JournalWriter
                                  @Task                         = @_FunctionName
                                , @FunctionName         = @_FunctionName
                                , @StepInFunction       = @_Step
                                , @MessageText          = @_Message
                                , @Severity                     = @_Severity
                                , @ExceptionId          = @_ExceptionId
                                --! Supply all the progress info after we've gone to such trouble to collect it
                                , @ExtraInfo        = @_ProgressText

        --! Finally, throw an exception that will be detected by the caller
        IF @DoThrowError = 1 AND @_Error > 0
                RAISERROR(@_Message, 16, 99);

        SET NOCOUNT OFF;

        --! Return the value of @@ERROR (which will be zero on success)
        RETURN (@_Error);
END