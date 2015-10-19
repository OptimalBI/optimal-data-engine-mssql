CREATE PROCEDURE [dbo].[dv_create_sat_table]
(
  @vault_database                varchar(128)   = NULL
, @vault_sat_name                varchar(128)   = NULL
, @recreate_flag                 char(1)		= 'N'
, @DoGenerateError               bit            = 0
, @DoThrowError                  bit			= 1
)
AS
BEGIN
SET NOCOUNT ON

DECLARE @crlf								char(2)			= CHAR(13) + CHAR(10)
-- Global Defaults
DECLARE  
		 @def_global_lowdate				datetime
        ,@def_global_highdate				datetime
        ,@def_global_default_load_date_time	varchar(128)
		,@def_global_failed_lookup_key		int
-- Hub Defaults									
        ,@def_hub_prefix					varchar(128)
		,@def_hub_schema					varchar(128)
		,@def_hub_filegroup					varchar(128)
--Link Defaults									
		,@def_link_prefix					varchar(128)
		,@def_link_schema					varchar(128)
		,@def_link_filegroup				varchar(128)
--Sat Defaults									
		,@def_sat_prefix					varchar(128)
		,@def_sat_schema					varchar(128)
		,@def_sat_filegroup					varchar(128)
		,@sat_start_date_col				varchar(128)
		,@sat_end_date_col					varchar(128)
		,@sat_current_row_col				varchar(128)				

-- Object Specific Settings
-- Source Table
		,@source_system						varchar(128)
		,@source_database					varchar(128)
		,@source_schema						varchar(128)
		,@source_table						varchar(128)
		,@source_table_config_key			int
		,@source_qualified_name				varchar(512)
		,@source_load_date_time				varchar(128)
		,@source_payload					nvarchar(max)
-- Hub Table
		,@hub_database						varchar(128)
		,@hub_schema						varchar(128)
		,@hub_table							varchar(128)
		,@hub_surrogate_keyname				varchar(128)
		,@hub_config_key					int
		,@hub_qualified_name				varchar(512)
		,@hubt_technical_columns			nvarchar(max)
-- Link Table
		,@link_database						varchar(128)
		,@link_schema						varchar(128)
		,@link_table						varchar(128)
		,@link_surrogate_keyname			varchar(128)
		,@link_config_key					int
		,@link_qualified_name				varchar(512)
		,@link_technical_columns			nvarchar(max)
		,@link_lookup_joins					nvarchar(max)
		,@link_hub_keys						nvarchar(max)
-- Sat Table
		,@sat_database						varchar(128)
		,@sat_schema						varchar(128)
		,@sat_table							varchar(128)
		,@sat_surrogate_keyname				varchar(128)
		,@hub_link_surrogate_key			varchar(128)
		,@sat_config_key					int
		,@sat_link_hub_flag					char(1)
		,@sat_qualified_name				varchar(512)
		,@sat_tombstone_indicator			varchar(50)
		,@sat_is_columnstore				bit
		,@sat_technical_columns				nvarchar(max)
		,@sat_payload						nvarchar(max)

-- Working Storage

DECLARE  @sql						nvarchar(max)
        ,@payload_columns           [dbo].[dv_column_type]

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
						+ @NEW_LINE + '    @vault_database               : ' + COALESCE(@vault_database, '<NULL>')
						+ @NEW_LINE + '    @vault_sat_name               : ' + COALESCE(@vault_sat_name, '<NULL>')
						+ @NEW_LINE + '    @recreate_flag                : ' + COALESCE(@recreate_flag, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF (select count(*) from [dbo].[dv_satellite] where [satellite_database] = @vault_database and [satellite_name] = @vault_sat_name) <> 1
			RAISERROR('Invalid Sat Name: %s', 16, 1, @vault_sat_name);
IF isnull(@recreate_flag, '') not in ('Y', 'N') 
			RAISERROR('Valid values for recreate_flag are Y or N : %s', 16, 1, @recreate_flag);
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Get required Parameters'

SET @_Step = 'Get Defaults'
-- System Wide Defaults
select
-- Global Defaults
 @def_global_lowdate				= cast([dbo].[fn_get_default_value] ('LowDate','Global')				as datetime)			
,@def_global_highdate				= cast([dbo].[fn_get_default_value] ('HighDate','Global')				as datetime)	
,@def_global_default_load_date_time	= cast([dbo].[fn_get_default_value] ('DefaultLoadDateTime','Global')	as varchar(128))
,@def_global_failed_lookup_key		= cast([dbo].[fn_get_default_value] ('FailedLookupKey', 'Global')     as integer)

-- Hub Defaults								
,@def_hub_prefix					= cast([dbo].[fn_get_default_value] ('prefix','hub')					as varchar(128))	
,@def_hub_schema					= cast([dbo].[fn_get_default_value] ('schema','hub')					as varchar(128))	
,@def_hub_filegroup					= cast([dbo].[fn_get_default_value] ('filegroup','hub')				as varchar(128))	
-- Link Defaults																						
,@def_link_prefix					= cast([dbo].[fn_get_default_value] ('prefix','lnk')					as varchar(128))	
,@def_link_schema					= cast([dbo].[fn_get_default_value] ('schema','lnk')					as varchar(128))	
,@def_link_filegroup				= cast([dbo].[fn_get_default_value] ('filegroup','lnk')				as varchar(128))	
-- Sat Defaults																							
,@def_sat_prefix					= cast([dbo].[fn_get_default_value] ('prefix','sat')					as varchar(128))	
,@def_sat_schema					= cast([dbo].[fn_get_default_value] ('schema','sat')					as varchar(128))	
,@def_sat_filegroup					= cast([dbo].[fn_get_default_value] ('filegroup','sat')				as varchar(128))

--Satellite Details
select @sat_start_date_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Version_Start_Date'
select @sat_end_date_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Version_End_Date'
select @sat_current_row_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Current_Row'

select @sat_tombstone_indicator = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Tombstone_Indicator'


-- Get Satellite Specifics
select 	 @sat_database			= sat.[satellite_database]						
		,@sat_schema			= coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')		
		,@sat_table				= sat.[satellite_name]		
		,@sat_surrogate_keyname	= [dbo].[fn_get_object_name] (sat.[satellite_name],'SatSurrogate')		
		,@sat_config_key		= sat.[satellite_key]		
		,@sat_link_hub_flag		= sat.[link_hub_satellite_flag]
		,@sat_is_columnstore	= sat.[is_columnstore]		
		,@sat_qualified_name	= quotename(sat.[satellite_database]) + '.' + quotename(coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] (sat.[satellite_name], 'sat')))       
from [dbo].[dv_column] c
inner join [dbo].[dv_satellite_column] sc
on sc.column_key = c.column_key
inner join [dbo].[dv_satellite] sat
on sat.satellite_key = sc.satellite_key
where 1=1
and sat.[satellite_database] = @vault_database
and sat.[satellite_name]	 = @vault_sat_name
and isnull(c.discard_flag, 0) <> 1 


if @sat_link_hub_flag = 'H'
insert @payload_columns
select top 1 k.[column_name]
       ,k.[column_type]
       ,k.[column_length]
	   ,k.[column_precision]
	   ,k.[column_scale]
	   ,k.[collation_Name]
	   ,k.[bk_ordinal_position]
       ,k.[ordinal_position]
	   ,0 --k.[satellite_ordinal_position]
	   ,''
	   ,''
  FROM [dbo].[dv_satellite] s
  inner join [dbo].[dv_satellite_column] hc
  on s.satellite_key = hc.satellite_key
  inner join [dbo].[dv_column] c
  on hc.column_key = c.column_key
  inner join [dbo].[dv_hub] h
  on s.[hub_key] = h.[hub_key]
  cross apply [dbo].[fn_get_key_definition] (h.hub_name, 'hub') k
  where s.[satellite_key] = @sat_config_key
else 
insert @payload_columns
select top 1 k.[column_name]
       ,k.[column_type]
       ,k.[column_length]
	   ,k.[column_precision]
	   ,k.[column_scale]
	   ,k.[collation_Name]
	   ,k.[bk_ordinal_position]
       ,k.[ordinal_position]
	   ,0 --k.[satellite_ordinal_position]
	   ,''
	   ,''
  FROM [dbo].[dv_satellite] s
  inner join [dbo].[dv_satellite_column] hc
  on s.satellite_key = hc.satellite_key
  inner join [dbo].[dv_column] c
  on hc.column_key = c.column_key
  inner join [dbo].[dv_link] l
  on s.[link_key] = l.[link_key]
  cross apply [dbo].[fn_get_key_definition] (l.link_name, 'lnk') k
  where s.[satellite_key] = @sat_config_key

select @hub_link_surrogate_key = [column_name] from @payload_columns

insert @payload_columns
select  c.[column_name]
       ,c.[column_type]
       ,c.[column_length]
	   ,c.[column_precision]
	   ,c.[column_scale]
	   ,c.[collation_Name]
	   ,c.[bk_ordinal_position]
       ,c.[source_ordinal_position]
	   ,c.[satellite_ordinal_position]
	   ,''
	   ,''
  FROM [dbo].[dv_satellite] s
  inner join [dbo].[dv_satellite_column] hc
  on s.satellite_key = hc.satellite_key
  inner join [dbo].[dv_column] c
  on hc.column_key = c.column_key
  where 1=1
  and s.[satellite_key] = @sat_config_key
  and isnull(c.discard_flag, 0) <> 1 

 /*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create The Sat'

EXECUTE [dbo].[dv_create_DV_table] 
   @sat_table
  ,@sat_schema
  ,@sat_database
  ,@def_sat_filegroup
  ,'Sat'
  ,@payload_columns
  ,@sat_is_columnstore
  ,@recreate_flag
  ,@dogenerateerror
  ,@dothrowerror

/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Index the Sat on the Surrogate Key Plus Row End Date'
select @SQL = ''
if @sat_is_columnstore = 0
	begin
	select @SQL += 'CREATE UNIQUE NONCLUSTERED INDEX ' + quotename('UX__' + @sat_table + cast(newid() as varchar(56))) 
	select @SQL += ' ON ' + @sat_qualified_name + '(' + @crlf + ' '
	--select @SQL = @SQL + @hub_link_surrogate_key + ',' + @sat_end_date_col +',' + @sat_current_row_col + ',' + @sat_tombstone_indicator	
	select @SQL = @SQL + @hub_link_surrogate_key + ',' + @sat_start_date_col 	
	select @SQL = @SQL + ') INCLUDE(' + @sat_current_row_col +',' + @sat_tombstone_indicator + ') ON ' + quotename(@def_sat_filegroup) + @crlf
	end
else
	begin
	select @SQL += 'CREATE CLUSTERED COLUMNSTORE INDEX ' + quotename('CCX__' + @sat_table + cast(newid() as varchar(56)))
	select @SQL += ' ON ' + @sat_qualified_name + @crlf + ' ' 
	end
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'Create The Index'
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @SQL + @crlf
--print @SQL
exec (@SQL)

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created Sat: ' + @sat_table

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create Sat: ' + @sat_table
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