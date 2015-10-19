
CREATE procedure [dv_integrity].[dv_check_sats_for_duplicate_keys]
(
  @dogenerateerror				bit				= 0
, @dothrowerror					bit				= 1
)
as
begin
set nocount on
declare 
	 @def_sat_schema		varchar(128)
	,@def_dv_rowstartdate		varchar(128)
	,@def_dv_row_is_current	varchar(128)
	,@def_dv_is_tombstone	varchar(128)
	,@sat_surrogate_keyname	varchar(128)
	,@sat_database			varchar(128)
	,@sat_qualified_name	varchar(512)

	,@sql					nvarchar(max)

declare @crlf				char(2) = CHAR(13) + CHAR(10)

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
SET @_JournalOnOff      = log4.GetJournalControl(@_FunctionName, 'IntegrityChecks');  -- left Group Name as HOWTO for now.


-- set Log4TSQL Parameters for Logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

--IF isnull(@vault_source_load_type, 'Full') not in ('Full', 'Delta')
--			RAISERROR('Invalid Load Type: %s', 16, 1, @vault_source_load_type);
--IF isnull(@recreate_flag, '') not in ('Y', 'N') 
--			RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'

select @_FunctionName      = isnull(OBJECT_NAME(@@PROCID), 'Test');
select @def_sat_schema	= cast([dbo].[fn_get_default_value]('schema','sat') as varchar)

select  @def_dv_rowstartdate	   	= [column_name] from [dbo].[dv_default_column] where object_type = 'sat' and object_column_type = 'Version_Start_Date'
select  @def_dv_row_is_current	= [column_name] from [dbo].[dv_default_column] where object_type = 'sat' and object_column_type = 'Current_Row'
select  @def_dv_is_tombstone	= [column_name] from [dbo].[dv_default_column] where object_type = 'sat' and object_column_type = 'Tombstone_Indicator'


SET @_Step = 'Build Cursor for all Satellites' 
declare cur_checks cursor for 
select 	 sat_surrogate_keyname	= case when link_hub_satellite_flag = 'H' then (select column_name from [dbo].[fn_get_key_definition](h.[hub_name],'Hub'))	
                                       when link_hub_satellite_flag = 'L' then (select column_name from [dbo].[fn_get_key_definition](l.[link_name],'Lnk'))
									   else '<Unknown>'
									   end		
		,sat_qualified_name		= quotename(s.[satellite_database]) + '.' + quotename(coalesce(s.[satellite_schema], @def_sat_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] (s.[satellite_name], 'sat')))       
		,sat_database = s.[satellite_database]
from [dbo].[dv_satellite] s
left join [dbo].[dv_hub] h
on h.hub_key = s.hub_key
and s.link_hub_satellite_flag = 'H' 
left join [dbo].[dv_link] l
on l.link_key = s.link_key
and s.link_hub_satellite_flag = 'L'
where 1=1
order by sat_qualified_name
				
open cur_checks
fetch next from cur_checks 
into 	 @sat_surrogate_keyname	
	 	,@sat_qualified_name
		,@sat_database

while @@FETCH_STATUS = 0
begin
SET @_Step = 'Checks on: ' + @sat_qualified_name

set @sql     = 'declare @xml1 varchar(max);'					+ @crlf +
               'select  @xml1 = ('								+ @crlf
select @sql += 'select s.'  + @sat_surrogate_keyname + ''		+ @crlf +
			   '      ,s.[' + @def_dv_rowstartdate	   + ']'	+ @crlf +
			   'from ' + @sat_qualified_name + ' s'				+ @crlf + 
			   'group by s.'  + @sat_surrogate_keyname + ''		+ @crlf +
			   '        ,s.[' + @def_dv_rowstartdate    + '] having count(*) > 1' + @crlf +
			   'order by s.'  + @sat_surrogate_keyname + ''		+ @crlf +
			   '        ,s.[' + @def_dv_rowstartdate    + ']'		+ @crlf +
			   'for xml auto)'									+ @crlf
set @sql = @sql
			+ 'if @xml1 is not null '							+ @crlf
            + 'EXECUTE [log4].[JournalWriter] @FunctionName = ''' + @_FunctionName + ''''
			+ ', @MessageText = ''Duplicate Keys Detected In - ' + @sat_qualified_name + ' - See [log4].[JournalDetail] for details''' 
			+ ', @ExtraInfo = @xml1' 
			+ ', @DatabaseName = ''' + @sat_database + ''''
			+ ', @Task = ''Duplicate Satellite Key Check'''
			+ ', @StepInFunction = ''' + @_Step + ''''
			+ ', @Severity = 256'
			+ ', @ExceptionId = 3601;' + @crlf
set @_ProgressText = @_ProgressText + 'Checked ' + @sat_qualified_name + ' for Duplicate Keys' + @crlf
--print @sql
EXECUTE sp_executesql @sql;

fetch next from cur_checks 
into 	 @sat_surrogate_keyname	
	 	,@sat_qualified_name
		,@sat_database
end

close cur_checks
deallocate cur_checks

set @_Step = 'Completed'

/**********************************************************************************************************************/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Ran Satellite Duplicate Key Checker' 

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Run Satellite Duplicate Key Checker' + @sat_qualified_name
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