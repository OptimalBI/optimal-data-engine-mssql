
CREATE PROCEDURE [dbo].[dv_load_source_table_key_lookup]
(
  @vault_source_unique_name             varchar(128) = NULL
, @link_load_only						char(1) = 'N'  -- "Y" Indicates that this temp table is being used for a link load key lookup, not a Sat key Lookup.
, @vault_source_load_type				varchar(50)  = NULL
, @vault_temp_table_name				varchar(116) OUTPUT
, @vault_sql_statement					nvarchar(max) OUTPUT
, @dogenerateerror                      bit		= 0
, @dothrowerror                         bit		= 1

)
AS
BEGIN
SET NOCOUNT ON

/**************************************************************************************************************************************************************
Creates a Code Block which takes the incoming stage table, adds the required Key Lookups and instantiates the result in a Temp Table.
Returns the Code Block plus the name of the Temp Table.
This Proc does not execute any code.

DECLARE @vault_source_unique_name varchar(128) = 'DLR_DTABN'
DECLARE @link_load_only char(1) = 'N'
DECLARE @vault_source_load_type varchar(50) = 'Delta'
DECLARE @vault_temp_table_name varchar(116)
DECLARE @vault_sql_statement nvarchar(max)

EXECUTE [dbo].[dv_load_source_table_key_lookup] 
   @vault_source_unique_name
  ,@link_load_only
  ,@vault_source_load_type
  ,@vault_temp_table_name OUTPUT
  ,@vault_sql_statement OUTPUT

PRINT @vault_temp_table_name
PRINT @vault_sql_statement
**************************************************************************************************************************************************************/

-- System Wide Defaults
-- Local Defaults Values
DECLARE @crlf											char(2) = CHAR(13) + CHAR(10)
-- Global Defaults
DECLARE
                 @def_global_lowdate                    datetime
				,@def_global_highdate                   datetime
				,@def_global_default_load_date_time     varchar(128)
                ,@def_global_failed_lookup_key          int
-- Hub Defaults
				,@def_hub_prefix                        varchar(128)
                ,@def_hub_schema                        varchar(128)
                ,@def_hub_filegroup                     varchar(128)
--Link Defaults
                ,@def_link_prefix                       varchar(128)
                ,@def_link_schema                       varchar(128)
                ,@def_link_filegroup                    varchar(128)
--Sat Defaults
                ,@def_sat_prefix                        varchar(128)
                ,@def_sat_schema                        varchar(128)
                ,@def_sat_filegroup                     varchar(128)
                ,@sat_start_date_col                    varchar(128)
                ,@sat_end_date_col                      varchar(128)


-- Object Specific Settings
-- Source Table
                ,@stage_system                          varchar(128)
				,@stage_database                        varchar(128)
                ,@stage_schema                          varchar(128)
                ,@stage_table                           varchar(128)
				,@stage_load_type					    varchar(50)
                ,@stage_table_config_key                int
				,@source_version_key				    int
                ,@stage_qualified_name                  varchar(512)
                ,@stage_load_date_time                  varchar(128)
                ,@stage_payload                         nvarchar(max)
				,@stage_hw_date_col						varchar(128)
				,@stage_hw_lsn_col						varchar(128)
				,@stage_cdc_start_date_col				varchar(128)
				,@stage_cdc_start_lsn					varchar(128)
				,@stage_cdc_sequence				varchar(128)
				,@stage_cdc_action						varchar(128)
-- Hub Table
                ,@hub_database                          varchar(128)
                ,@hub_schema                            varchar(128)
                ,@hub_table                             varchar(128)
                ,@hub_surrogate_keyname                 varchar(128)
                ,@hub_config_key                        int
                ,@hub_qualified_name                    varchar(512)
                ,@hubt_technical_columns                nvarchar(max)
-- Link Table
                ,@link_database                         varchar(128)
                ,@link_schema                           varchar(128)
                ,@link_table                            varchar(128)
                ,@link_surrogate_keyname                varchar(128)
                ,@link_config_key                       int
                ,@link_qualified_name                   varchar(512)
                ,@link_technical_columns                nvarchar(max)
                ,@link_lookup_joins                     nvarchar(max)
                ,@link_hub_keys                         nvarchar(max)
-- Sat Table
                ,@sat_database                          varchar(128)
                ,@sat_schema                            varchar(128)
                ,@sat_table                             varchar(128)
                ,@sat_surrogate_keyname                 varchar(128)
                ,@sat_config_key                        int
                ,@sat_link_hub_flag                     char(1)
				,@sat_duplicate_removal_threshold		int			
                ,@sat_qualified_name                    varchar(512)
                ,@sat_technical_columns                 nvarchar(max)
                ,@sat_payload                           nvarchar(max)

--  Working Storage
DECLARE @sat_insert_count								int
       ,@temp_table_name_001							varchar(116)
       ,@sql											nvarchar(max)
       ,@sql1											nvarchar(max)
       ,@sql2											nvarchar(max)
       ,@surrogate_key_match							nvarchar(max)
DECLARE @declare										nvarchar(512)   = ''
DECLARE @count_rows										nvarchar(256)   = ''
DECLARE @match_list										nvarchar(max)   = ''
DECLARE @value_list										nvarchar(max)   = ''
DECLARE @sat_column_list								nvarchar(max)   = ''
DECLARE @hub_column_list								nvarchar(max)   = ''

DECLARE @ParmDefinition									nvarchar(500);

DECLARE @wrk_link_joins									nvarchar(max)
DECLARE @wrk_hub_joins									nvarchar(max)
DECLARE @wrk_link_keys									nvarchar(max)


DECLARE @ref_function_seq								int
       ,@column_name									varchar(128)
	   ,@ref_function_name								varchar(128)
	   ,@ref_function									nvarchar(4000)
	   ,@func_arguments									nvarchar(512)
	   ,@func_ordinal_position							int
declare @input_string									nvarchar(4000)
       ,@output_string									nvarchar(4000)
	   ,@lastwith										varchar(100)

declare @c_hub_key										int
		,@c_hub_name									varchar(128)
		,@c_hub_schema									varchar(128)
		,@c_hub_database								varchar(128)
		,@c_link_key_name								varchar(128)
		,@c_link_key_column_key							int
		--,@c_hub_data_type								varchar(128)

declare @ref_function_list table (ref_function_seq		int identity(1,1)
								 ,column_name			varchar(128)
								 ,ref_function_name		varchar(128)
								 ,ref_function			nvarchar(4000)
								 ,[func_arguments]		nvarchar(512)
								 ,func_ordinal_position int
								 ,with_statement		nvarchar(4000))

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
                                                 + @NEW_LINE + '    @vault_source_unique_name  : ' + COALESCE(@vault_source_unique_name, 'NULL')
                                                 + @NEW_LINE + '    @link_load_only            : ' + COALESCE(@link_load_only, 'NULL')
												 + @NEW_LINE + '    @vault_source_load_type    : ' + COALESCE(@vault_source_load_type, 'NULL')
                                                 + @NEW_LINE + '    @DoGenerateError           : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
                                                 + @NEW_LINE + '    @DoThrowError              : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
                                                 + @NEW_LINE

BEGIN TRY

SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF isnull(@vault_source_load_type, '') not in ('Full', 'Delta')
			RAISERROR('Invalid Load Type: %s', 16, 1, @vault_source_load_type);
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
-- Hub Defaults
,@def_hub_prefix                                = cast([dbo].[fn_get_default_value] ('prefix','hub')                  as varchar(128))
,@def_hub_schema                                = cast([dbo].[fn_get_default_value] ('schema','hub')                  as varchar(128))
,@def_hub_filegroup                             = cast([dbo].[fn_get_default_value] ('filegroup','hub')               as varchar(128))
-- Link Defaults
,@def_link_prefix                               = cast([dbo].[fn_get_default_value] ('prefix','lnk')                  as varchar(128))
,@def_link_schema                               = cast([dbo].[fn_get_default_value] ('schema','lnk')                  as varchar(128))
,@def_link_filegroup                            = cast([dbo].[fn_get_default_value] ('filegroup','lnk')               as varchar(128))
-- Sat Defaults
,@def_sat_prefix                                = cast([dbo].[fn_get_default_value] ('prefix','sat')                  as varchar(128))
,@def_sat_schema                                = cast([dbo].[fn_get_default_value] ('schema','sat')                  as varchar(128))
,@def_sat_filegroup                             = cast([dbo].[fn_get_default_value] ('filegroup','sat')               as varchar(128))

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
select @stage_hw_date_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'CdcStgODE'
and object_column_type = 'CDC_HighWaterDate'
select @stage_hw_lsn_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'CdcStgMSSQL'
and object_column_type = 'CDC_HighWaterlsn'
select @stage_cdc_start_date_col = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'CdcStgODE'
and object_column_type = 'CDC_StartDate'

select @stage_cdc_start_lsn = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'CdcStgMSSQL'
and object_column_type = 'CDC_StartLSN'
select @stage_cdc_sequence = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type = 'CdcStgMSSQL'
and object_column_type = 'CDC_Sequence'




-- Object Specific Settings
-- Source Table
select   @stage_database                        = sdb.[stage_database_name]
        ,@stage_schema                          = ss.[stage_schema_name]
        ,@stage_table                           = st.[stage_table_name]
        ,@stage_table_config_key				= st.[source_table_key]
        ,@stage_qualified_name					= quotename(sdb.[stage_database_name]) + '.' + quotename(ss.[stage_schema_name]) + '.' + quotename(st.[stage_table_name])
		,@stage_load_type						= st.load_type
		,@source_version_key                    = sv.source_version_key
from [dbo].[dv_source_table] st
inner join [dbo].[dv_stage_schema] ss on ss.stage_schema_key = st.stage_schema_key
inner join [dbo].[dv_stage_database] sdb on sdb.stage_database_key = ss.stage_database_key
left join  [dbo].[dv_source_version] sv on sv.source_table_key = st.source_table_key
										and sv.is_current = 1
where 1=1
and st.[source_unique_name]						= @vault_source_unique_name



select @stage_cdc_action = [column_name]
from [dbo].[dv_default_column]
	where 1=1
and object_type = CASE @stage_load_type when'ODEcdc' then 'CdcStgODE' else 'CdcStgMSSQL' end
and [object_column_type]  = 'CDC_Action'


-- Satellite
select	   @sat_config_key						= sat.[satellite_key]
          ,@sat_link_hub_flag					= sat.[link_hub_satellite_flag]
		  ,@sat_duplicate_removal_threshold		= sat.[duplicate_removal_threshold]
from [dbo].[dv_source_table] t
inner join [dbo].[dv_column] c
on c.table_key = t.[source_table_key]
inner join [dbo].[dv_satellite_column] sc
on sc.satellite_col_key = c.satellite_col_key
inner join [dbo].[dv_satellite] sat
on sat.satellite_key = sc.satellite_key
where 1=1
and t.[source_table_key] = @stage_table_config_key

-- Owner Hub Table
if @sat_link_hub_flag = 'H'
        select   @hub_database                  = h.[hub_database]
                ,@hub_schema                    = coalesce([hub_schema], @def_hub_schema, 'dbo')
                ,@hub_table                     = h.[hub_name]
				,@hub_surrogate_keyname			= (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](h.[hub_name], 'hub'))
                ,@hub_config_key                = h.[hub_key]
                ,@hub_qualified_name			= quotename([hub_database]) + '.' + quotename(coalesce([hub_schema], @def_hub_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([hub_name], 'hub')))
        from [dbo].[dv_satellite] s
        inner join [dbo].[dv_hub] h
        on s.hub_key = h.hub_key
where 1=1
and s.[satellite_key] = @sat_config_key

-- Owner Link Table
if @sat_link_hub_flag = 'L'
begin
        select   @link_database                 = l.[link_database]
                ,@link_schema                   = coalesce(l.[link_schema], @def_link_schema, 'dbo')
                ,@link_table                    = l.[link_name]
				,@link_surrogate_keyname		= (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](l.[link_name], 'lnk'))
                ,@link_config_key               = l.[link_key]
                ,@link_qualified_name			= quotename([link_database]) + '.' + quotename(coalesce(l.[link_schema], @def_link_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([link_name], 'lnk')))
        from [dbo].[dv_satellite] s
        inner join [dbo].[dv_link] l
        on s.link_key = l.link_key
    where 1=1
    and s.[satellite_key] = @sat_config_key

        set @link_lookup_joins = ''
        set @link_hub_keys = ''


	set @link_hub_keys		= ''
	set @wrk_link_keys		= ''
	set @link_lookup_joins	= ''
	set @wrk_hub_joins		= ''
	set @wrk_link_joins     = ''

	DECLARE c_hub_key CURSOR FOR
	select distinct 
	       h.[hub_key]
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
	where 1=1
	  and l.[link_key] = @link_config_key
	  order by [link_key_name]
	OPEN c_hub_key
	FETCH NEXT FROM c_hub_key
	INTO @c_hub_key
		,@c_hub_name
		,@c_hub_schema
		,@c_hub_database
		,@c_link_key_name
		,@c_link_key_column_key
		--,@c_hub_data_type

	WHILE @@FETCH_STATUS = 0
	BEGIN
 			select @wrk_link_joins  = 'LEFT JOIN ' + quotename(@c_hub_database) + '.' + quotename(coalesce(@c_hub_schema, @def_hub_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] (@c_hub_name, 'hub'))) + ' ' + @c_link_key_name + @crlf + ' ON  '
			
			select @wrk_link_keys  += ' tmp.' + (select column_name from [dbo].[fn_get_key_definition](@c_link_key_name, 'hub')) + 
				     				  ' = link.' + (select column_name from [dbo].[fn_get_key_definition](@c_link_key_name, 'hub')) + @crlf + ' AND '
			select @wrk_link_joins += @c_link_key_name + '.' + quotename(hkc.[hub_key_column_name]) + ' = ' +
									 case when  hub_data_type <> col_data_type
										  then col_data_type_cast
										  else 'src.' + quotename(hkc.[column_name])
										  end + @crlf + ' AND '  

			 from (
			 select distinct 
			 h.[hub_name]
			,col_data_type = [dbo].[fn_build_column_definition] ('',c.[column_type],c.[column_length],c.[column_precision],c.[column_scale],c.[Collation_Name],0,NULL,0,0,0,0)
			,col_data_type_cast = [dbo].[fn_build_column_definition] ('src.' + quotename(c.[column_name]), hkc.[hub_key_column_type],hkc.[hub_key_column_length],hkc.[hub_key_column_precision],hkc.[hub_key_column_scale],hkc.[hub_key_Collation_Name],0,NULL,0,0,1,0) 
			,hub_data_type = [dbo].[fn_build_column_definition] ('', hkc.[hub_key_column_type],hkc.[hub_key_column_length],hkc.[hub_key_column_precision],hkc.[hub_key_column_scale],hkc.[hub_key_Collation_Name],0,NULL,0,0,0,0)
			,hkc.[hub_key_column_name]
			,hkc.hub_key_ordinal_position
			,c.[column_name]
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
			and hc.link_key_column_key = @c_link_key_column_key
			and st.[source_table_key] = @stage_table_config_key
			and c.is_retired <> 1) hkc
			ORDER BY hkc.hub_key_ordinal_position
---------------------------------------------------------
			select  @wrk_hub_joins += ', ' + @c_link_key_name + '.' + (select column_name from [dbo].[fn_get_key_definition]([hub_name], 'hub')) + ' as ' + 
									(select column_name from [dbo].[fn_get_key_definition](@c_link_key_name, 'hub')) + @crlf				   
			from(
			select distinct hub_name
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
			and c.[is_retired] <> 1) hkc
			-------------------
			set @link_lookup_joins = @link_lookup_joins + left(@wrk_link_joins, len(@wrk_link_joins) - 4)

			FETCH NEXT FROM c_hub_key
			INTO @c_hub_key
					,@c_hub_name
					,@c_hub_schema
					,@c_hub_database
					,@c_link_key_name
					,@c_link_key_column_key
					--,@c_hub_data_type
	END

	CLOSE c_hub_key
	DEALLOCATE c_hub_key
	select @wrk_link_keys = left(@wrk_link_keys, len(@wrk_link_keys) - 4)
end

---- Use either a date time from the source or the default
select @stage_load_date_time = [column_name]
from [dbo].[dv_source_table] st
inner join [dbo].[dv_column] c
on st.[source_table_key] = c.table_key
where 1=1
and st.[source_table_key] = @stage_table_config_key
and c.[is_source_date] = 1
--NB do not check Discard Flag here as the Date Column may not be included in the Sat.
if @@rowcount > 1 RAISERROR ('Source Table has Multiple Source Dates Defined',16,1);
select @stage_load_date_time = isnull(@stage_load_date_time, @def_global_default_load_date_time)

-- Build the Source Payload NB - needs to join to the Sat Table to get each satellite related to the source.
set @sql = ''
select @sql += quotename(sc.column_name) + ' = ' + 
           case when c.[is_retired] = 1 
				then 'NULL'
		        when  [dbo].[fn_build_column_definition] ('',c.[column_type],c.[column_length],c.[column_precision],c.[column_scale],c.[Collation_Name],0,NULL,0,0,0,0)
		            = [dbo].[fn_build_column_definition] ('',sc.[column_type],sc.[column_length],sc.[column_precision],sc.[column_scale],sc.[Collation_Name],0,NULL,0,0,0,0)
				then 'src.' + quotename(c.[column_name])
				else [dbo].[fn_build_column_definition] ('src.' + quotename(c.[column_name]),sc.[column_type],sc.[column_length],sc.[column_precision],sc.[column_scale],sc.[Collation_Name],0,NULL,0,0,1,0)
				end 
			+ @crlf +', '
from [dbo].[dv_column] c
inner join [dbo].[dv_satellite_column] sc on sc.satellite_col_key = c.satellite_col_key
where 1=1
   and c.[table_key] = @stage_table_config_key
   --and sc.[satellite_col_key] <> 0
   and sc.[satellite_col_key] is not null
order by c.source_ordinal_position
select @stage_payload = left(@sql, len(@sql) -1)

---- Build the Sat Payload
set @sql = '' 
select @sql += 'sat.' +quotename([column_name]) + @crlf +', '
from [dbo].[dv_default_column]
where 1=1
and object_column_type <> 'Object_Key'
and [object_type] = 'Sat'
order by [ordinal_position]
set @sat_technical_columns = @sql

-- Temp Tables

select @temp_table_name_001 = '##temp_001_' + replace(cast(newid() as varchar(50)), '-', '')

-- Build the SQL to obtain Surrogate Keys, before Merging the Sat.
-- HUB based

-- Get the Key Match
if @sat_link_hub_flag = 'H'
begin
        select @sql = ''
        select @sql += 'hub.' + quotename(hkc.[hub_key_column_name]) + ' = ' +
		       case when  [dbo].[fn_build_column_definition] ('',c.[column_type],c.[column_length],c.[column_precision],c.[column_scale],c.[Collation_Name],0,NULL,0,0,0,0)
		                = [dbo].[fn_build_column_definition] ('',[hub_key_column_type],[hub_key_column_length],[hub_key_column_precision],[hub_key_column_scale],[hub_key_Collation_Name],0,NULL,0,0,0,0)
				then 'src.' + quotename(c.[column_name])
				else [dbo].[fn_build_column_definition] ('src.' + quotename(c.[column_name]),[hub_key_column_type],[hub_key_column_length],[hub_key_column_precision],[hub_key_column_scale],[hub_key_Collation_Name],0,NULL,0,0,1,0)
				end  
		        + @crlf + ' AND '
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
        and h.hub_key = @hub_config_key
        and st.[source_table_key] = @stage_table_config_key
        and c.[is_retired] <> 1
        ORDER BY hkc.hub_key_ordinal_position
        select @surrogate_key_match =  left(@sql, len(@sql) - 4)
end

-- Compile the SQL
-- If it is a link, create the temp table with all Hub keys plus a dummy for the Link Keys.
--set @sql1 = 'DECLARE @lookup_start_date datetimeoffset(7) = sysdatetimeoffset()' + @crlf 
set @sql1 = ''
select @sql1 = @sql1 + [dv_scripting].[fn_get_task_log_insert_statement] ('', '', '', 1)  -- get the logging variables.



if @stage_load_type in('ODEcdc', 'MSSQLcdc')
	begin
	set @sql1 = @sql1 + 'DECLARE @counter			INT' + @crlf
	set @sql1 = @sql1 + '		,@loopmax			INT' + @crlf
	if @stage_load_type = 'ODEcdc'
		set @sql1 = @sql1 + 'select top 1 @__source_high_water_date = CAST(' + @stage_hw_date_col + ' AS VARCHAR(50)) FROM ' + @stage_qualified_name + @crlf
	else 
		--set @sql1 = @sql1 + 'select top 1 @__source_high_water_lsn = CAST(' + @stage_hw_lsn_col + ' AS BINARY(10)) FROM ' + @stage_qualified_name + @crlf
		set @sql1 = @sql1 + 'select top 1 @__source_high_water_lsn = ' + @stage_hw_lsn_col + ' FROM ' + @stage_qualified_name + @crlf
	end

set @sql1 = @sql1 + 'DECLARE @version_date DATETIMEOFFSET(7)' + @crlf
set @sql1 = @sql1 + '       ,@source_date_time DATETIMEOFFSET(7)' + @crlf
set @sql1 = @sql1 + 'SELECT @__load_start_date = SYSDATETIMEOFFSET()' + @crlf

set @sql1 = @sql1 +  ';WITH wBaseSet as (' + @crlf

if @sat_link_hub_flag = 'H'
        set @sql1 += 'SELECT ' + quotename(@hub_surrogate_keyname) + ' = isnull(hub.' + quotename(@hub_surrogate_keyname) + ', ' + cast(@def_global_failed_lookup_key as varchar(50)) + ')' + @crlf

if @sat_link_hub_flag = 'L'
    begin
        set @sql1 += 'SELECT DISTINCT ' + quotename(@link_surrogate_keyname) + ' = cast(0 as integer) ' + @crlf
        set @sql1 = @sql1 + @wrk_hub_joins
    end

if not (@link_load_only = 'Y' and @sat_link_hub_flag = 'L')
        set @sql1 = @sql1 + ', ' + @stage_payload
set @sql1 = @sql1 + ', [vault_load_time] = ' + @stage_load_date_time + @crlf

--**********************************************************************************************************************************************************************************
-- If its a CDC Load (and Delta / Increment) then 
--     add in the "Action" column and
--     number the updates per key for later processing of multiple updates.

if @stage_load_type IN('ODEcdc', 'MSSQLcdc') and @vault_source_load_type = 'Delta'
    begin
	set @sql1 = @sql1 + ', ' + @stage_cdc_action + @crlf
	set @sql1 = @sql1 + ', [rn] = ROW_NUMBER() OVER (PARTITION BY ' + 
	     'isnull(' + case when @sat_link_hub_flag = 'H' then 'hub.' + quotename(@hub_surrogate_keyname) else 'link.' + quotename(@link_surrogate_keyname) end + ', ' + cast(@def_global_failed_lookup_key as varchar(50)) + ')' +
		 ' ORDER BY ' + 
		 case when @stage_load_type = 'ODEcdc' then @stage_cdc_start_date_col else @stage_cdc_start_lsn + ', ' + @stage_cdc_sequence end
		 + ')' + @crlf
	end
set @sql1 = @sql1 + 'FROM ' + @stage_qualified_name + ' src' + @crlf

if @sat_link_hub_flag = 'H'
        set @sql1 = @sql1 + 'LEFT JOIN ' + @hub_qualified_name + ' hub' + ' ON ' + @surrogate_key_match + @crlf
if @sat_link_hub_flag = 'L'
        set @sql1 = @sql1 + @link_lookup_joins

set @sql1 = @sql1 + ')' + @crlf
----  Check for any functions and build the With code to add them onto the select

insert @ref_function_list
select sc.column_name
      ,f.ref_function_name
         ,f.ref_function
         ,sc.[func_arguments]
         ,sc.func_ordinal_position
         ,'' 
from [dbo].[dv_satellite_column] sc
inner join [dbo].[dv_ref_function] f
on f.[ref_function_key] = sc.[ref_function_key]
where f.[ref_function_key] is not null
and sc.satellite_key in
	(select distinct sc.[satellite_key]
	from [dbo].[dv_source_table] st
	inner join [dbo].[dv_column] c on c.table_key = st.source_table_key
	inner join [dbo].[dv_satellite_column] sc on sc.[satellite_col_key] = c.[satellite_col_key]
	and st.[source_table_key] = @stage_table_config_key)
order by sc.func_ordinal_position
        ,sc.column_name

declare curFunc CURSOR LOCAL for
select [ref_function_seq]
      ,[column_name]
	  ,[ref_function]
	  ,[func_arguments]
from @ref_function_list
open curFunc
fetch next from curFunc into  @ref_function_seq,@column_name, @ref_function, @func_arguments
while @@FETCH_STATUS = 0 
BEGIN
    set @input_string = @ref_function
	EXECUTE [dv_scripting].[dv_build_snippet] 
		@input_string
	   ,@func_arguments
	   ,@output_string OUTPUT
	
    select @output_string = ', w' + cast(@ref_function_seq as varchar(10)) + ' as (select *, '  + @output_string + ' as ' + quotename(@column_name) + ' from w' + 
							case when @ref_function_seq = 1 then 'BaseSet' else cast(@ref_function_seq -1 as varchar(10)) end + ')' + @crlf
	update @ref_function_list set with_statement = @output_string where ref_function_seq = @ref_function_seq
	fetch next from curFunc into @ref_function_seq,@column_name, @ref_function, @func_arguments
END
close curFunc
deallocate curFunc
set @sql = ''
select @sql += with_statement from @ref_function_list order by ref_function_seq
select @lastwith = 'w' + isnull(cast(max(ref_function_seq) as varchar), 'BaseSet') from @ref_function_list
set @sql1 += @sql

set @sql1 = @sql1 +'SELECT * ' + ' INTO ' + @temp_table_name_001 + ' FROM ' + @lastwith + ';'

if (@sat_link_hub_flag = 'L' and @link_load_only <> 'Y')
        begin
        set @sql1 = @sql1 + @crlf + 'UPDATE tmp ' + @crlf
        set @sql1 = @sql1 + 'SET tmp.' + quotename(@link_surrogate_keyname) + ' = isnull(link.' + quotename(@link_surrogate_keyname) + ', ' + cast(@def_global_failed_lookup_key as varchar(50)) + ')' + @crlf
        set @sql1 = @sql1 + 'FROM ' + @temp_table_name_001 + ' tmp' + @crlf
        set @sql1 = @sql1 + 'LEFT JOIN ' + @link_qualified_name + ' link ' + @crlf + ' ON ' + @wrk_link_keys + ';'
        end
set @sql1 = @sql1 + @crlf + 'SELECT @__rows_inserted = @@ROWCOUNT' + @crlf
set @sql1 = @sql1 + 'SELECT @__load_end_date = SYSDATETIMEOFFSET()' + @crlf

/****************************************************************************************************************************************/
-- Duplicate Checking
-- Note - add checking for Delta / Incremental Run when CDC - full CDC runs can still check for duplicates.

if (@sat_link_hub_flag = 'H' or (@sat_link_hub_flag = 'L' and @link_load_only <> 'Y')) and not (@stage_load_type in('ODEcdc', 'MSSQLcdc') and @vault_source_load_type = 'Delta')
        begin
        if @sat_duplicate_removal_threshold > 0
        begin
        set @sql1 = @sql1 + 'select ''' + @temp_table_name_001 + ''' as global_temp_table_name, * into #t1 from ' + @temp_table_name_001 + ' where ' + case when @sat_link_hub_flag = 'H' then quotename(@hub_surrogate_keyname) else @link_surrogate_keyname end  + ' in( ' + @crlf
        set @sql1 = @sql1 + '        select top ' + cast(@sat_duplicate_removal_threshold + 1 as varchar) + case when @sat_link_hub_flag = 'H' then quotename(@hub_surrogate_keyname) else @link_surrogate_keyname end  + ' from '
                          + @temp_table_name_001 + ' group by ' + case when @sat_link_hub_flag = 'H' then quotename(@hub_surrogate_keyname) else @link_surrogate_keyname end  + ' having count(*) > 1)'  + @crlf
        set @sql1 = @sql1 + 'IF (select count(*) from #t1) > 0 ' + @crlf
        set @sql1 = @sql1 + '    begin' + @crlf
        set @sql1 = @sql1 + '    declare @xml1 varchar(max);' + @crlf
        set @sql1 = @sql1 + '    select  @xml1 = (select * from #t1 order by 2 for xml auto);' + @crlf
        set @sql1 = @sql1 + '    EXECUTE [log4].[JournalWriter]  @FunctionName = ''' + @_FunctionName + ''''
                                                            + ', @MessageText = ''Duplicate Keys Removed while Loading - ' + @stage_qualified_name + ' - See [log4].[JournalDetail] for details'''
                                                            + ', @ExtraInfo = @xml1'
                                                            + ', @DatabaseName = ''' + @stage_database + ''''
                                                            + ', @Task = ''Key Lookup before Loading Source Table'''
                                                            + ', @StepInFunction = ''Remove Duplicates before Loading Source Table'''
                                                            + ', @Severity = 256'
                                                            + ', @ExceptionId = 3601;' + @crlf

        set @sql1 = @sql1 + '    IF (select count(*) from #t1) >  ' + cast(@sat_duplicate_removal_threshold as varchar) + @crlf
        set @sql1 = @sql1 + '        raiserror (''Duplicate Keys Detected while Loading ' + @stage_qualified_name + '''' + ', 16, 1)' + @crlf
        set @sql1 = @sql1 + '    else' + @crlf
        set @sql1 = @sql1 + '    DELETE FROM ' + @temp_table_name_001 + ' WHERE ' + case when @sat_link_hub_flag = 'H' then quotename(@hub_surrogate_keyname) else @link_surrogate_keyname end  + ' IN(' + @crlf
        set @sql1 = @sql1 + '           select distinct ' +  case when @sat_link_hub_flag = 'H' then quotename(@hub_surrogate_keyname) else @link_surrogate_keyname end  + ' FROM #t1); ' + @crlf
        set @sql1 = @sql1 + '    end' + @crlf

        end
        else
                set @sql1 = @sql1 + 'if exists (select 1 from ' + @temp_table_name_001 + ' group by ' + case when @sat_link_hub_flag = 'H' then quotename(@hub_surrogate_keyname) else @link_surrogate_keyname end  + ' having count(*) > 1)' + @crlf + '    raiserror (''Duplicate Keys Detected while Loading ' + @stage_qualified_name + '''' + ', 16, 1)' + @crlf + @crlf
        end

if @sat_link_hub_flag = 'H'
	select @sql1 += [dv_scripting].[fn_get_task_log_insert_statement] (@source_version_key, 'hublookup', @hub_config_key, 0)
else if @sat_link_hub_flag = 'L'
	select @sql1 += [dv_scripting].[fn_get_task_log_insert_statement] (@source_version_key, 'linklookup', @link_config_key, 0)		
/****************************************************************************************************************************************/
set @vault_sql_statement        = @sql1
select @vault_temp_table_name   = @temp_table_name_001
IF @_JournalOnOff = 'ON' SET @_ProgressText = @crlf + @vault_sql_statement + @crlf
/*--------------------------------------------------------------------------------------------------------------*/
--print @vault_sql_statement
/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
                                + 'Step: [' + @_Step + '] completed '

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Loaded Keys For: ' + @stage_qualified_name

END TRY
BEGIN CATCH
SET @_ErrorContext      = 'Failed to Load Keys For: ' + @stage_qualified_name
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