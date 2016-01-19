
CREATE PROCEDURE [dbo].[dv_satellite_hashdiff_refresh]
(
  @vault_satellite_name					varchar(128) = NULL
, @vault_current_rows_only				bit     = 0
, @dogenerateerror                      bit		= 0
, @dothrowerror                         bit		= 1
)
AS
BEGIN
SET NOCOUNT ON

-- Local Defaults Values
DECLARE @crlf											char(2) = CHAR(13) + CHAR(10)
-- Global Defaults
DECLARE
                 @def_global_lowdate                    datetime
				,@def_global_highdate                   datetime
				,@def_global_default_load_date_time     varchar(128)
                ,@def_global_failed_lookup_key          int
-- Hub Defaults
                --,@def_hub_schema                        varchar(128)
--Link Defaults
               -- ,@def_link_schema                       varchar(128)
--Sat Defaults
                ,@def_sat_prefix                        varchar(128)
                ,@def_sat_schema                        varchar(128)
                ,@def_sat_filegroup                     varchar(128)
                ,@sat_start_date_col                    varchar(128)
                ,@sat_end_date_col                      varchar(128)
				,@sat_hashmatching_col					varchar(128)
				,@def_sat_hashmatching_type				varchar(128)
				,@def_sat_hashmatching_delimiter		varchar(10)
				,@def_sat_IsColumnStore					int

-- Object Specific Settings
-- Source Table
				,@source_hash_payload					nvarchar(max)
-- Sat Table
                ,@sat_database                          varchar(128)
                ,@sat_schema                            varchar(128)
                ,@sat_table                             varchar(128)
				,@sat_tombstone_indicator				varchar(128)
				,@sat_hashmatching_type					varchar(10)	
				,@sat_hashmatching_char_length          int
                ,@sat_qualified_name                    varchar(512)
                ,@sat_payload                           nvarchar(max)



--  Working Storage
DECLARE @sat_insert_count								int
       ,@sql											nvarchar(max)

-- Log4TSQL Journal Constants
DECLARE @SEVERITY_CRITICAL								smallint = 1;
DECLARE @SEVERITY_SEVERE								smallint = 2;
DECLARE @SEVERITY_MAJOR									smallint = 4;
DECLARE @SEVERITY_MODERATE								smallint = 8;
DECLARE @SEVERITY_MINOR									smallint = 16;
DECLARE @SEVERITY_CONCURRENCY							smallint = 32;
DECLARE @SEVERITY_INFORMATION							smallint = 256;
DECLARE @SEVERITY_SUCCESS								smallint = 512;
DECLARE @SEVERITY_DEBUG									smallint = 1024;
DECLARE @NEW_LINE										char(1)  = CHAR(10);

-- Log4TSQL Standard/ExceptionHandler variables
DECLARE	@_Error											int
      , @_RowCount										int
      , @_Step											varchar(128)
      , @_Message										nvarchar(512)
      , @_ErrorContext									nvarchar(512)

-- Log4TSQL JournalWriter variables
DECLARE			  @_FunctionName                        varchar(255)
                , @_SprocStartTime                      datetime
                , @_JournalOnOff                        varchar(3)
                , @_Severity                            smallint
                , @_ExceptionId                         int
                , @_StepStartTime                       datetime
                , @_ProgressText                        nvarchar(max)

SET @_Error             = 0;
SET @_FunctionName      = OBJECT_NAME(@@PROCID);
SET @_Severity          = @SEVERITY_INFORMATION;
SET @_SprocStartTime    = sysdatetimeoffset();
SET @_ProgressText      = ''
SET @_JournalOnOff      = log4.GetJournalControl(@_FunctionName, 'HOWTO');  -- left Group Name as HOWTO for now.

-- set Log4TSQL Parameters for Logging:
SET @_ProgressText              = @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
                                                 + @NEW_LINE + '    @vault_satellite_name      : ' + COALESCE(@vault_satellite_name, 'NULL')
												 + @NEW_LINE + '    @vault_current_rows_only   : ' + COALESCE(CAST(@vault_current_rows_only AS varchar), 'NULL')
                                                 + @NEW_LINE + '    @DoGenerateError           : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
                                                 + @NEW_LINE + '    @DoThrowError              : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
                                                 + @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

--IF (select count(*) from [dbo].[dv_sat] where sat_name = @sat_name) <> 1
--                      RAISERROR('Invalid sat Name: %s', 16, 1, @sat_name);
--IF isnull(@recreate_flag, '') not in ('Y', 'N')
--                      RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'
-- System Wide Defaults
select
-- Global Defaults
 @def_global_lowdate                            = cast([dbo].[fn_get_default_value] ('LowDate','Global')              as datetime)
,@def_global_highdate                           = cast([dbo].[fn_get_default_value] ('HighDate','Global')             as datetime)
,@def_global_default_load_date_time				= cast([dbo].[fn_get_default_value] ('DefaultLoadDateTime','Global')  as varchar(128))
,@def_global_failed_lookup_key					= cast([dbo].[fn_get_default_value] ('FailedLookupKey', 'Global')     as integer)

-- Sat Defaults
,@def_sat_hashmatching_type						= cast([dbo].[fn_get_default_value] ('HashMatchingType','sat')		  as varchar) 
,@def_sat_hashmatching_delimiter				= cast([dbo].[fn_get_default_value] ('HashMatchingDelimiter','sat')	  as varchar)
,@def_sat_IsColumnStore							= cast([dbo].[fn_get_default_value] ('IsColumnStore','sat')			  as integer)

select @sat_start_date_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'sat'
and object_column_type = 'Version_Start_Date'
select @sat_end_date_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'sat'
and object_column_type = 'Version_End_Date'

select @sat_tombstone_indicator = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Tombstone_Indicator'


SET @_Step = 'Get Satellite Details'
-- Satellite

select top 1
		   @sat_hashmatching_type			    = coalesce(sat.[hashmatching_type], @def_sat_hashmatching_type, 'None')
		  ,@sat_qualified_name	= quotename(sat.[satellite_database]) + '.' + quotename(coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] (sat.[satellite_name], 'sat'))) 
from [dbo].[dv_satellite] sat
where 1=1
and sat.[satellite_name] = @vault_satellite_name

if coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type, 'None') <> 'None'
	select @sat_hashmatching_col = [column_name]		   
	from [dbo].[dv_default_column]
	where 1=1
	and [object_type] = 'Sat'
	and [object_column_type] <> 'Object_Key' 
	and [object_column_type] = coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type) + '_match'

select @sat_hashmatching_char_length = column_length from [dbo].[dv_default_column]
where 1=1
and [object_type] = 'Sat'
and [object_column_type] <> 'Object_Key' 
and [object_column_type] = coalesce(@sat_hashmatching_type, @def_sat_hashmatching_type) + '_match'



-- Build The Hashing Statement
SET @_Step = 'Compile the SQL'
set @sql = ''
if isnull(@sat_hashmatching_type, 'None') <> 'None'
begin
	set @sql = 'upper(convert(char(' + cast(@sat_hashmatching_char_length as varchar) + '), hashbytes(''' + @sat_hashmatching_type + ''', concat(' + @crlf 
	select @sql += 'ltrim(rtrim(isnull(cast(' + quotename(c.[column_name]) + ' as nvarchar), ''''))), ''' + @def_sat_hashmatching_delimiter + ''',' + @crlf 
	from [dbo].[dv_column] c
	inner join [dbo].[dv_satellite_column] sc on sc.[column_key] = c.[column_key]
	inner join [dbo].[dv_satellite] s on s.satellite_key = sc.satellite_key
	where 1=1
	and c.[discard_flag] <> 1
	and s.[satellite_name] = @vault_satellite_name
	order by c.[satellite_ordinal_position], c.[column_name]
	select @source_hash_payload = left(@sql, len(@sql) -3) + ')),2))'

	set @sql = ''
	set @sql += 'UPDATE ' + @sat_qualified_name + @crlf
	set @sql += 'SET ' + @sat_hashmatching_col + ' = ' + @crlf + 'CASE WHEN ' + @sat_tombstone_indicator + ' = 1  THEN ''<Tombstone>'' ' + @crlf + 'ELSE ' + @source_hash_payload + ' END' + @crlf
	if isnull(@vault_current_rows_only, 0) = 1
		set @sql += 'WHERE ' + @sat_end_date_col + ' = ''' + cast(@def_global_highdate as varchar) + '''' + @crlf
end 

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Updating HashDiffs For: ' + @sat_qualified_name
IF @_JournalOnOff = 'ON'
      SET @_ProgressText += @sql
--print @sql
EXECUTE sp_executesql @sql;
/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
                                + 'Step: [' + @_Step + '] completed '

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Updated HashDiffs For: ' + @sat_qualified_name

END TRY
BEGIN CATCH
SET @_ErrorContext      = 'Failed to Update HashDiffs For: ' + @sat_qualified_name
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