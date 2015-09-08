CREATE PROCEDURE [dbo].[dv_load_source_table]
(
  @vault_source_system_name		varchar(128) = NULL
, @vault_source_table_schema	varchar(128) = NULL
, @vault_source_table_name		varchar(128) = NULL
, @dogenerateerror				bit				= 0
, @dothrowerror					bit				= 1
)
AS
BEGIN
SET NOCOUNT ON

-- To Do - add Logging for the Payload Parameter
--         validate Parameters properly
--         Add Transactionality for Multi Sat Loads
--         Pick a single sat to load (from a multi sat)
-- System Wide Defaults
-- Local Defaults Values

set nocount on

-- Object Specific Settings
-- Source Table
declare  @source_system						varchar(128)
        ,@source_database					varchar(128)
		,@source_schema						varchar(128)
		,@source_table						varchar(128)
		,@source_table_config_key			int
		,@source_qualified_name				varchar(512)
		,@source_load_date_time				varchar(128)
		,@source_payload					nvarchar(max)
--  Working Storage
declare @load_details table 
       (source_database_name		varchar(128)
       ,source_table_key		int
	   ,source_table_schema     varchar(128)
	   ,source_table_name		varchar(128)
	   ,source_table_load_type  varchar(50)
	   ,satellite_database      varchar(128)
	   ,satellite_name			varchar(128)
	   ,link_key				int
	   ,link_name				varchar(128)
	   ,link_database			varchar(128)
	   ,hub_key					int
	   ,hub_name				varchar(128)
	   ,hub_database			varchar(128))
	   
	   
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


-- set Log4TSQL Parameters for Logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @vault_source_system_name     : ' + COALESCE(@vault_source_system_name, 'NULL')
						+ @NEW_LINE + '    @vault_source_table_schema    : ' + COALESCE(@vault_source_table_schema, 'NULL')
						+ @NEW_LINE + '    @vault_source_table_name      : ' + COALESCE(@vault_source_table_name, 'NULL')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY

SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

--IF (select count(*) from [dbo].[dv_sat] where sat_name = @sat_name) <> 1
--			RAISERROR('Invalid sat Name: %s', 16, 1, @sat_name);
--IF isnull(@recreate_flag, '') not in ('Y', 'N') 
--			RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/	   
SET @_Step = 'Get Defaults'
-- Object Specific Settings
-- Source Table
select 	 @source_system				= s.[source_system_name]	
        ,@source_database			= s.[timevault_name]
		,@source_schema				= t.[source_table_schema]
		,@source_table				= t.[source_table_name]
		,@source_table_config_key	= t.[table_key]
		,@source_qualified_name		= quotename(s.[timevault_name]) + '.' + quotename(t.[source_table_schema]) + '.' + quotename(t.[source_table_name])
from [dbo].[dv_source_system] s
inner join [dbo].[dv_source_table] t
on t.system_key = s.system_key
where 1=1
and s.[source_system_name]		= @vault_source_system_name
and t.[source_table_schema]		= @vault_source_table_schema
and t.[source_table_name]		= @vault_source_table_name

SET @_Step = 'Get All Components related to the Source Table'	    
insert @load_details            
select  distinct
        ss.timevault_name
       ,st.table_key as source_table_key
       ,st.source_table_schema	
	   ,st.source_table_name	
	   ,st.source_table_load_type
	   ,s.satellite_database
	   ,s.satellite_name
	   ,l.link_key
	   ,l.link_name
	   ,l.link_database
	   ,case when s.link_hub_satellite_flag = 'H' then h.hub_key		else linkhub.hub_key		end as hub_key
	   ,case when s.link_hub_satellite_flag = 'H' then h.hub_name		else linkhub.hub_name		end as hub_name
	   ,case when s.link_hub_satellite_flag = 'H' then h.hub_database	else linkhub.hub_database	end as hub_database 
from
[dbo].[dv_source_system] ss
inner join [dbo].[dv_source_table] st
on st.system_key = ss.system_key
inner join [dbo].[dv_column] c
on c.table_key = st.table_key
inner join [dbo].[dv_satellite_column] sc
on sc.column_key = c.column_key
inner join [dbo].[dv_satellite] s
on s.satellite_key = sc.satellite_key
left join [dbo].[dv_link] l
on s.link_key = l.link_key
and l.link_key > 0
left join [dbo].[dv_hub] h 
on h.hub_key = s.hub_key
and h.hub_key > 0
left join [dbo].[dv_hub_link] hl
on hl.link_key = l.link_key
left join [dbo].[dv_hub] linkhub
on linkhub.hub_key = hl.hub_key
where ss.source_system_name		= @vault_source_system_name
  and st.source_table_schema	= @vault_source_table_schema
  and st.source_table_name		= @vault_source_table_name

/*****************************************************************************************************************/
--Load the Source Table in stages:
--declare @source_database		varchar(128)
--       ,@source_table_schema	varchar(128)
--	   ,@source_table_name		varchar(128)

--select @source_database		= source_database_name
--      ,@source_table_schema = source_table_schema
--	  ,@source_table_name	= source_table_name
--from @load_details
/*****************************************************************************************************************/
SET @_Step = 'Load Hub Tables'
--print ''
--print 'Load Hub Tables'
--print '----------------'
declare @hub_database			varchar(128)
       ,@hub_name				varchar(128)

DECLARE hub_cursor CURSOR FOR  
select distinct 
       hub_database
	  ,hub_name
from @load_details

OPEN hub_cursor   
FETCH NEXT FROM hub_cursor INTO @hub_database, @hub_name  

WHILE @@FETCH_STATUS = 0   
BEGIN   
       SET @_Step = 'Load Hub: ' + @hub_name
	   --print @hub_name
	   --print '/*********\'
	   
	   EXECUTE [dbo].[dv_load_hub_table] @source_system, @source_schema, @source_table, @hub_database, @hub_name
	   FETCH NEXT FROM hub_cursor INTO @hub_database, @hub_name  
END   

CLOSE hub_cursor   
DEALLOCATE hub_cursor


/*****************************************************************************************************************/
SET @_Step = 'Load Link Tables'
--print ''
--print 'Load Link Tables'
--print '----------------'
declare @link_database			varchar(128)
       ,@link_name				varchar(128)

DECLARE link_cursor CURSOR FOR  
select distinct 
       link_database
	  ,link_name
from @load_details

OPEN link_cursor   
FETCH NEXT FROM link_cursor INTO @link_database, @link_name  

WHILE @@FETCH_STATUS = 0   
BEGIN   
       SET @_Step = 'Load Link: ' + @link_name
	   --print @link_name
	   --print '/*********\'
	   EXECUTE [dbo].[dv_load_link_table] @source_system, @source_schema, @source_table, @link_database, @link_name 
	   FETCH NEXT FROM link_cursor INTO @link_database, @link_name  
END   

CLOSE link_cursor   
DEALLOCATE link_cursor


/*****************************************************************************************************************/
SET @_Step = 'Load Sat Tables for: ' + @source_database + ' ' + @source_schema + ' ' + @source_table
--print ''
--print 'Load Sat Tables'
--print '----------------'
--print '/*********\'
EXECUTE [dv_load_sats_for_source_table] @source_system, @source_schema, @source_table
/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Loaded Object: ' + @source_qualified_name

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Load Object: ' + @source_qualified_name
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