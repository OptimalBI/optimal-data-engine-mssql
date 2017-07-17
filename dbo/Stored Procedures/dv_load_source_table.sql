CREATE PROCEDURE [dbo].[dv_load_source_table]
(
  @vault_source_unique_name		varchar(128) = NULL
, @vault_source_load_type		varchar(50)  = NULL
, @vault_runkey                 int          = NULL
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
declare  @source_database					varchar(128)
		,@source_schema						varchar(128)
		,@source_table						varchar(128)
		,@source_load_type					varchar(50)
		,@source_type						varchar(50)
		,@source_table_config_key			int
		,@source_qualified_name				varchar(512)
		,@source_version_key				int
		,@source_version					int
		,@source_procedure_name				varchar(128)
		,@source_load_date_time				varchar(128)
		,@source_payload					nvarchar(max)
		,@error_message						varchar(256) 
		,@stage_delta_switch				varchar(100) = 'N'
		,@sql								nvarchar(4000)
--  Working Storage
declare @load_details table 
       (source_table_key		int
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
						+ @NEW_LINE + '    @vault_source_unique_name     : ' + COALESCE(@vault_source_unique_name, 'NULL')
						+ @NEW_LINE + '    @vault_source_load_type       : ' + COALESCE(@vault_source_load_type, 'NULL')
						+ @NEW_LINE + '    @vault_runkey                 : ' + COALESCE(CAST(@vault_runkey AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY

SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF isnull(@vault_source_load_type, '') not in ('Full', 'Delta')
			RAISERROR('Invalid Load Type: %s', 16, 1, @vault_source_load_type);
IF ((@vault_runkey is not null) and ((select count(*) from [dv_scheduler].[dv_run] where @vault_runkey = [run_key]) <> 1))
			RAISERROR('Invalid @vault_runkey provided: %i', 16, 1, @vault_runkey);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get Defaults'
-- Global Settings
select @stage_delta_switch		= [default_varchar] from [dbo].[dv_defaults]		where default_type = 'Global'	and default_subtype = 'StageDeltaSwitch'

-- Object Specific Settings
-- Source Table
select 	 @source_database			= sdb.[stage_database_name]
		,@source_schema				= ss.[stage_schema_name]
		,@source_table				= st.[stage_table_name]
		--,@source_load_type			= CASE st.[load_type] 
		--								WHEN 'Full'		THEN 'Full'     -- if the Load is set up to be full, then it will always be a Full Load
		--                                ELSE COALESCE(@vault_source_load_type, st.[load_type], 'Full')
		--								END										    
		--,@source_load_type			= coalesce(@vault_source_load_type, st.[load_type], 'Full')
		,@source_load_type			= @vault_source_load_type
		,@source_type				= sv.[source_type]
		,@source_table_config_key	= st.[source_table_key]
		,@source_qualified_name		= quotename(sdb.[stage_database_name]) + '.' + quotename(ss.[stage_schema_name]) + '.' + quotename(st.[stage_table_name])
		,@source_version			= sv.[source_version]
		,@source_procedure_name		= sv.[source_procedure_name]
from [dbo].[dv_source_table] st
inner join [dbo].[dv_stage_schema] ss on ss.stage_schema_key = st.stage_schema_key
inner join [dbo].[dv_stage_database] sdb on sdb.stage_database_key = ss.stage_database_key
inner join [dbo].[dv_source_version] sv on sv.[source_table_key] = st.[source_table_key]
where 1=1
and st.[source_unique_name]		= @vault_source_unique_name
and sv.[is_current]				= 1
if @@ROWCOUNT <> 1 RAISERROR('dv_source_table or current dv_source_version missing for : %s', 16, 1, @vault_source_unique_name);

SET @_Step = 'Get All Components related to the Source Table'	    
          
insert @load_details
select  distinct
        st.[source_table_key] as source_table_key
	   ,l.link_key
	   ,l.link_name
	   ,l.link_database
	   ,null,null,null
from
           [dbo].[dv_source_table] st		
inner join [dbo].[dv_column] c				on c.table_key = st.[source_table_key] 
inner join [dbo].[dv_hub_column] hc			on hc.column_key = c.column_key
inner join [dbo].[dv_link_key_column] lkc	on lkc.link_key_column_key = hc.link_key_column_key
inner join [dbo].[dv_link] l				on l.link_key = lkc.link_key
											and (l.link_key > 0 OR l.link_key < -100)
where st.source_table_key = @source_table_config_key
  
union

select  distinct
       st.[source_table_key] as source_table_key
	   ,null,null,null
	   ,h.hub_key
	   ,h.hub_name
	   ,h.hub_database
from
           [dbo].[dv_source_table] st		
inner join [dbo].[dv_column] c				on c.table_key = st.[source_table_key] 
inner join [dbo].[dv_hub_column] hc			on hc.column_key = c.column_key
inner join [dbo].[dv_hub_key_column] hkc	on hkc.hub_key_column_key = hc.hub_key_column_key
inner join [dbo].[dv_hub] h					on h.hub_key = hkc.hub_key
											and (h.hub_key > 0 OR h.hub_key < -100)
where st.source_table_key = @source_table_config_key

;with wBaseSet as (
select source_table_key
      ,count(distinct hub_name) as hub
	  ,count(distinct link_name) as link
from @load_details
group by source_table_key)
select @error_message = 
       case when link > 1 then 'Source Table is configured to load more than 1 Link.'
            when link < 1 then case when hub <> 1 then 'Source Table which loads a Hub may only load 1 Hub' else 'Success' end
			when link = 1 then case when hub < 1 then 'Link must be configured to link one or more Hubs' else 'Success' end
			else 'Success'
			end
from wBaseSet 
if @error_message <> 'Success' raiserror('Incorrect Source configuration: %s', 16, 1, @error_message)
--select * from @load_details
/*****************************************************************************************************************/
SET @_Step = 'Process the Stage Table'
print ''
print 'Process Stage Table'
print '-------------------'
EXECUTE[dbo].[dv_load_stage_table] 
   @vault_source_unique_name	= @vault_source_unique_name
  ,@vault_source_load_type		= @source_load_type
  ,@vault_runkey				= @vault_runkey
  ,@vault_source_version_key	= @source_version_key OUTPUT -- returns the Source Version Key to be applied as "Source" to the Vault Objects being loaded later.
 
/*****************************************************************************************************************/
SET @_Step = 'Load Hub Tables'
print ''
print 'Load Hub Tables'
print '----------------'
declare @hub_database			varchar(128)
       ,@hub_name				varchar(128)

DECLARE hub_cursor CURSOR FOR  
select distinct 
       hub_database
	  ,hub_name
from @load_details
where hub_name is not null

OPEN hub_cursor   
FETCH NEXT FROM hub_cursor INTO @hub_database, @hub_name  

WHILE @@FETCH_STATUS = 0   
BEGIN   
       SET @_Step = 'Load Hub: ' + @hub_name
	   print @hub_name
	   EXECUTE [dbo].[dv_load_hub_table] @vault_source_unique_name, @hub_database, @hub_name, @source_version_key, @vault_runkey
	   FETCH NEXT FROM hub_cursor INTO @hub_database, @hub_name  
END   

CLOSE hub_cursor   
DEALLOCATE hub_cursor


/*****************************************************************************************************************/
SET @_Step = 'Load Link Tables'
print ''
print 'Load Link Tables'
print '----------------'
declare @link_database			varchar(128)
       ,@link_name				varchar(128)

DECLARE link_cursor CURSOR FOR  
select distinct 
       link_database
	  ,link_name
from @load_details
where link_name is not null

OPEN link_cursor   
FETCH NEXT FROM link_cursor INTO @link_database, @link_name  

WHILE @@FETCH_STATUS = 0   
BEGIN   
       SET @_Step = 'Load Link: ' + @link_name
	   print ''
	   print @link_name
	   EXECUTE [dbo].[dv_load_link_table] @vault_source_unique_name, @link_name, @source_version_key, @vault_runkey
	   FETCH NEXT FROM link_cursor INTO @link_database, @link_name  
END   

CLOSE link_cursor   
DEALLOCATE link_cursor


/*****************************************************************************************************************/
SET @_Step = 'Load Sat Tables for: ' + @vault_source_unique_name
print ''
print 'Load Sat Tables'
print '----------------'
print @_Step
EXECUTE [dv_load_sats_for_source_table] @vault_source_unique_name, @source_load_type,@source_version_key, @vault_runkey
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