
CREATE PROCEDURE [dv_integrity].[dv_col_metrics]
(
   @satellite_key				int				= 0
  ,@stage_database				varchar(128)	= 'ODE_Metrics_Stage'
  ,@stage_schema				varchar(128)	= 'Stage'
  ,@stage_table					varchar(128)	= 'Integrity_Col_Counts'
  ,@dogenerateerror				bit				= 0
  ,@dothrowerror				bit				= 1
)
as
begin
set nocount on

-- Local Defaults Values
declare @crlf char(2) = char(13) + char(10)
--Global Defaults
DECLARE  
		 @def_global_lowdate				datetime
        ,@def_global_highdate				datetime
        ,@def_global_default_load_date_time	varchar(128)

-- Proc Defaults
declare  @def_runtype						varchar(128)
--Sat Defaults									
declare  @sat_tombstone_col					varchar(128)
		,@sat_current_row_col				varchar(128)
		,@sat_start_date_col				varchar(128)
		,@sat_end_date_col					varchar(128)	
-- Sat Table
declare  @sat_database						varchar(128)
		,@sat_schema						varchar(128)
		,@sat_table							varchar(128)
		,@sat_qualified_name				varchar(512)
declare @Tests		table (seq int, test_name varchar(50), test_prefix varchar(10), test_type varchar(10), test_template varchar(256), test_column varchar(128))
declare @Columns	table (satellite_key bigint,satellite_name varchar(256),table_name varchar(1024),column_key bigint, column_name varchar(256), test_name varchar(50), test_script varchar(max), test_column varchar(128), test_seq int)   

-- Stage Table
declare  @stage_qualified_name				varchar(512)

--  Working Storage
declare @sql								nvarchar(max) = ''
declare @sql1								nvarchar(max) = ''
declare @sql2								nvarchar(max) = ''
declare @sqlprint							varchar(max)  = ''
declare @run_time							varchar(50)
declare @col_loop_key						varchar(128)
declare @sat_loop_key						bigint
declare @sat_loop_stop_key					bigint

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
select @_FunctionName   = isnull(OBJECT_NAME(@@PROCID), 'Test');

-- set Log4TSQL Parameters for Logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @satellite_key                : ' + COALESCE(CAST(@satellite_key AS varchar), 'NULL') 
						+ @NEW_LINE + '    @stage_database               : ' + @stage_database					
						+ @NEW_LINE + '    @stage_schema                 : ' + @stage_schema				
						+ @NEW_LINE + '    @stage_table                  : ' + @stage_table					    
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

/*--------------------------------------------------------------------------------------------------------------*/
select
-- Global Defaults
 @def_global_lowdate				= cast([dbo].[fn_get_default_value] ('LowDate','Global')							as datetime)			
,@def_global_highdate				= cast([dbo].[fn_get_default_value] ('HighDate','Global')							as datetime)	
,@def_global_default_load_date_time	= cast([dbo].[fn_get_default_value] ('DefaultLoadDateTime','Global')				as varchar(128))
-- Proc Defaults
,@def_runtype						= cast(isnull([dbo].[fn_get_default_value] ('RunType', 'dv_col_metrics'), 'Full')	as varchar(128))
-- overide the default to Full if a specific Satellite was requested:
if isnull(@satellite_key, 0) > 0
	set @def_runtype = 'Full'
-- Sat Defaults
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
select @sat_tombstone_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Tombstone_Indicator'

--stage values
set @stage_qualified_name = quotename(@stage_database) + '.' + quotename(@stage_schema) + '.' + quotename(@stage_table) 
select @run_time = cast(sysdatetimeoffset() as varchar(50))

--tests:
-- Note that each test must have a case for each data type being references (it must be a regular matrix)
-- Needs to go into Config!
insert @tests values(10, 'min'		, 'min'			, 'varchar'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'nvarchar', 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'char'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'numeric'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'decimal'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'int'		, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'bigint'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'smallint', 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'tinyint'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'datetime', 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'date'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'money'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'smallmoney'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'float'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'real'	, 'cast(min([<column_name>]) as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')
insert @tests values(10, 'min'		, 'min'			, 'bit'		, 'cast(0 as varchar(max)) as [min_<column_name>]', '[min_<column_name>]')

insert @tests values(20, 'max'		, 'max'			, 'varchar'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'nvarchar', 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'char', 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'numeric'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'decimal'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'int'		, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'bigint'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'smallint', 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'tinyint'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'datetime', 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'date'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'money'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'smallmoney'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'float'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'real'	, 'cast(max([<column_name>]) as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')
insert @tests values(20, 'max'		, 'max'			, 'bit'		, 'cast(0 as varchar(max)) as [max_<column_name>]', '[max_<column_name>]')

insert @tests values(30, 'domain'	, 'domain'		, 'varchar' , 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'nvarchar', 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'char'	, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'numeric'	, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'decimal'	, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'int'		, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'bigint'	, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'smallint', 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'tinyint'	, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'datetime', 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'date'	, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'money'	, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'smallmoney'	, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'float'	, 'cast(0 as bigint) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'real'	, 'cast(0 as bigint) as [domain_<column_name>]'	 , '[domain_<column_name>]')
insert @tests values(30, 'domain'	, 'domain'		, 'bit'		, 'count_big(distinct [<column_name>]) as [domain_<column_name>]'	 , '[domain_<column_name>]')

insert @tests values(40, 'nullcount', 'nullcount'	, 'varchar' , 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'nvarchar', 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'char'	, 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'numeric' , 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'decimal' , 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'int'		, 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'bigint'  , 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'smallint', 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'tinyint' , 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'datetime', 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'date'	, 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'money'	, 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'smallmoney'	, 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'float'	, 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'real'	, 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')
insert @tests values(40, 'nullcount', 'nullcount'	, 'bit'		, 'sum(cast(case when [<column_name>] is null then 1 else 0 end as bigint)) as [nullcount_<column_name>]', '[nullcount_<column_name>]')

insert @tests values(50, 'blankcount', 'blankcount'	, 'varchar' , 'sum(cast(case when [<column_name>] = '''' then 1 else 0 end as bigint)) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'nvarchar','sum(cast(case when [<column_name>] = '''' then 1 else 0 end as bigint)) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'char'	,'sum(cast(case when [<column_name>] = '''' then 1 else 0 end as bigint)) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'numeric' , 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'decimal' , 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'int'		, 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'bigint'  , 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'smallint', 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'tinyint' , 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'datetime', 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'date'	, 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'money'	, 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'smallmoney'	, 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'float'	, 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'real'	, 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')
insert @tests values(50, 'blankcount', 'blankcount'	, 'bit'		, 'cast(0 as bigint) as [blankcount_<column_name>]', '[blankcount_<column_name>]')

insert @tests values(60, 'minlength', 'minlength'	, 'varchar' , 'min(cast(len([<column_name>]) as bigint)) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'nvarchar','min(cast(len([<column_name>]) as bigint)) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'char'	,'min(cast(len([<column_name>]) as bigint)) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'numeric' , 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'decimal' , 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'int'		, 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'bigint'  , 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'smallint', 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'tinyint' , 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'datetime', 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'date'	, 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'money'	, 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'smallmoney'	, 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'float'	, 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'real'	, 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')
insert @tests values(60, 'minlength', 'minlength'	, 'bit'		, 'cast(0 as bigint) as [minlength_<column_name>]', '[minlength_<column_name>]')

insert @tests values(70, 'maxlength', 'maxlength'	, 'varchar' , 'max(cast(len([<column_name>]) as bigint)) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'nvarchar','max(cast(len([<column_name>]) as bigint)) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'char'	,'max(cast(len([<column_name>]) as bigint)) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'numeric' , 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'decimal' , 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'int'		, 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'bigint'  , 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'smallint', 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'tinyint' , 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'datetime', 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'date'	, 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'money'	, 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'smallmoney'	, 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'float'	, 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'real'	, 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')
insert @tests values(70, 'maxlength', 'maxlength'	, 'bit'		, 'cast(0 as bigint) as [maxlength_<column_name>]', '[maxlength_<column_name>]')

-- Truncate the Stage Table
/*--------------------------------------------------------------------------------------------------------------*/
SET @_Step = 'If Exists then Drop Existing Table'

BEGIN
       select @sql = 'IF EXISTS (select 1 from ' + quotename(@stage_database) + '.INFORMATION_SCHEMA.TABLES where TABLE_TYPE = ''BASE TABLE'' and TABLE_SCHEMA = ''' + @stage_schema + ''' and TABLE_NAME = ''' + @stage_table + ''')' + @crlf + ' '
	   select @sql += 'DROP TABLE ' + @stage_qualified_name + @crlf + ' '
       select @sql += 'CREATE TABLE ' + @stage_qualified_name + '(
       [runtime] [varchar](34) NOT NULL,
       [sat_key] [int] NOT NULL,
       [sat_name] [varchar](128) NOT NULL,
       [column_name] [varchar](128) NOT NULL,
       [column_key] [varchar](128) NOT NULL,
	   [min_value] [varchar](max) NULL,
       [max_value] [varchar](max) NULL,
       [domain_count] [bigint] NULL,
       [null_count] [bigint] NULL,
       [blank_count] [bigint] NULL,
       [minlength] [bigint] NULL,
	   [maxlength] [bigint] NULL
       )'
       execute sp_executesql @sql
       set @sql = ''
END
/*--------------------------------------------------------------------------------------------------------------*/
-- Build the test SQL

select @sat_loop_key = case when isnull(@satellite_key, 0) = 0 then max(satellite_key) else @satellite_key end 
	from [dbo].[dv_satellite] 
	where is_retired = 0
set @sat_loop_stop_key = isnull(@satellite_key, 0)
while @sat_loop_key >= @sat_loop_stop_key
/**********************************************************************************************************************/
begin
-- Looping through all Satellites
	if (@def_runtype = 'Weekly' and @sat_loop_key%7 = datepart(weekday, getdate()-7)-1
		)
	   or @def_runtype = 'Full'
	begin 
		delete from @Columns
		insert @Columns
		  select s.satellite_key
				,s.satellite_name
				,table_name = quotename(s.[satellite_database]) + '.' + quotename(s.[satellite_schema]) + '.' + quotename([dbo].[fn_get_object_name] (s.satellite_name, 'sat'))
				,sc.satellite_col_key
				,sc.column_name
				,t.test_name
				,replace(t.test_template,'<column_name>', sc.column_name) as  test_script
				,replace(t.test_column,'<column_name>', sc.column_name) as  test_column
				,t.seq as test_seq
		  from [dbo].[dv_satellite] s
		  inner join [dbo].[dv_satellite_column] sc
		  on sc.[satellite_key] = s.[satellite_key]
		  inner join @tests t
		  on sc.[column_type] = t.[test_type]
		  where 1=1
			and s.satellite_key = @sat_loop_key
			and sc.column_length >= 0
		--select * from @tests   
		if @@rowcount > 0
		begin
			select @sql1 = ';with w1 as ('
						 + 'select ''' + @run_time + ''' as [runtime]' + @crlf +
						 + ',' + cast(satellite_key as varchar(20)) + ' as [sat_key]' + @crlf 
						 + ',''' + satellite_name + ''' as [sat_name]' + @crlf
						 + ',''Sat'' as [object_type]' + @crlf
			from @Columns
			select @sql1 += ',' + test_script
						 + @crlf
			from @Columns
			order by satellite_name, column_name
			select @sql2 = 'from ' + table_name + @crlf from @Columns
			set @sql2 += 'where ' + @def_global_default_load_date_time + ' >= ' + @sat_start_date_col + ' and ' +  @def_global_default_load_date_time + ' < ' + @sat_end_date_col + @crlf
					   + 'and ' + @sat_tombstone_col + '= 0)' + @crlf
					   + ',w2 as (' + @crlf
			select @col_loop_key  = min(column_name) from @Columns
			while @col_loop_key is not null
			begin 
			--Looping through all Columns
				select @sql2 = @sql2 + 'select ' + @crlf
					  + ' runtime' + @crlf	
					  + ',sat_key' + @crlf	
					  + ',sat_name' + @crlf
					  + ',''' + @col_loop_key + ''' as [column_name]' + @crlf
				select @sql2 = @sql2 + ',''' + cast(column_key as varchar(20)) + ''' as [column_key]' + @crlf
					  from (select distinct column_key from @Columns where column_name = @col_loop_key) a
				select @sql2 += ',' + [test_column] + @crlf
					  from @Columns where column_name = @col_loop_key
					  order by test_seq asc
				select @sql2 += 'from w1' + @crlf + 'union' + @crlf
				select @col_loop_key = min(column_name) from @Columns where column_name > @col_loop_key 
			end
			set @sql2 = left(@sql2, len(@sql2) - 7)
			set @sql1 = @sql + @sql1 + @sql2 
			set @sql1 = @sql1 + ')' + @crlf
             + 'insert ' + @stage_qualified_name + @crlf
             + 'select * from w2'
			select @sqlprint = cast(@sql1 as varchar(max))
			--print @sqlprint 
            execute sp_executesql @sql1
			IF @_JournalOnOff = 'ON' SET @_ProgressText = @crlf + @sql1 + @crlf
		end
	end
	select @sat_loop_key = max(satellite_key) from [dbo].[dv_satellite]
			where satellite_key < @sat_loop_key 
end
/**********************************************************************************************************************/

SET @_Step = 'Extract the Stats'
IF @_JournalOnOff = 'ON' SET @_ProgressText  = @_ProgressText + @crlf + @sql1 + @crlf

set @_Step = 'Completed'

/**********************************************************************************************************************/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Ran Column Integrity Checker' 

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Run Column Integrity Checker' + @sat_qualified_name
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