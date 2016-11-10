CREATE PROCEDURE [dbo].[dv_validate_hub_sat]
(
  @vault_source_system_name		varchar(128) = NULL
, @vault_source_table_schema	varchar(128) = NULL
, @vault_source_table_name		varchar(128) = NULL
, @vault_sat_name				varchar(128) = NULL
)
AS
BEGIN
SET NOCOUNT ON
/*
select * from [dbo].[dv_satellite] where link_hub_satellite_flag = 'H'
select * from [dbo].[dv_source_table]
select * from [dbo].[dv_source_system]
*/
--declare 
--  @vault_source_system_name		varchar(128) = 'Adventureworks'
--, @vault_source_table_schema	varchar(128) = 'Stage'
--, @vault_source_table_name		varchar(128) = 'Adventureworks__Sales__vIndividualCustomer'
--, @vault_sat_name				varchar(128) = 'Adventureworks__Sales__vIndividualCustomer'


DECLARE @crlf											char(2) = CHAR(13) + CHAR(10)
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
		,@sat_tombstone_indicator			varchar(50)				

-- Object Specific Settings
-- Source Table
		,@source_system						varchar(128)
		,@source_database					varchar(128)
		,@source_schema						varchar(128)
		,@source_table						varchar(128)
		,@source_table_config_key			int
		,@source_qualified_name				varchar(512)
		,@source_load_date_time				varchar(128)
		,@source_load_type					varchar(50)
		,@source_payload					nvarchar(max)

		-- Hub Table
		,@hub_database						varchar(128)
		,@hub_schema						varchar(128)
		,@hub_table							varchar(128)
		,@hub_surrogate_keyname				varchar(128)
		,@hub_config_key					int
		,@hub_qualified_name				varchar(512)
		,@hub_technical_columns				nvarchar(max)

-- Sat Table
		,@sat_database						varchar(128)
		,@sat_schema						varchar(128)
		,@sat_table							varchar(128)
		,@sat_surrogate_keyname				varchar(128)
		,@sat_config_key					int
		,@sat_link_hub_flag					char(1)
		,@sat_qualified_name				varchar(512)
		,@sat_source_date_time				varchar(50)
		,@sat_technical_columns				nvarchar(max)
		,@sat_payload						nvarchar(max)

declare  @SQL								nvarchar(max)	

select
-- Global Defaults
 @def_global_lowdate				= cast([dbo].[fn_get_default_value] ('LowDate','Global')				as datetime)			
,@def_global_highdate				= cast([dbo].[fn_get_default_value] ('HighDate','Global')				as datetime)	
,@def_global_default_load_date_time	= cast([dbo].[fn_get_default_value] ('DefaultLoadDateTime','Global')	as varchar(128))
,@def_global_failed_lookup_key		= cast([dbo].[fn_get_default_value] ('FailedLookupKey', 'Global')		as integer)
-- Hub Defaults								
,@def_hub_prefix					= cast([dbo].[fn_get_default_value] ('prefix','hub')					as varchar(128))	
,@def_hub_schema					= cast([dbo].[fn_get_default_value] ('schema','hub')					as varchar(128))	
,@def_hub_filegroup					= cast([dbo].[fn_get_default_value] ('filegroup','hub')					as varchar(128))	
-- Link Defaults																						
,@def_link_prefix					= cast([dbo].[fn_get_default_value] ('prefix','lnk')					as varchar(128))	
,@def_link_schema					= cast([dbo].[fn_get_default_value] ('schema','lnk')					as varchar(128))	
,@def_link_filegroup				= cast([dbo].[fn_get_default_value] ('filegroup','lnk')					as varchar(128))	
-- Sat Defaults																							
,@def_sat_prefix					= cast([dbo].[fn_get_default_value] ('prefix','sat')					as varchar(128))	
,@def_sat_schema					= cast([dbo].[fn_get_default_value] ('schema','sat')					as varchar(128))	
,@def_sat_filegroup					= cast([dbo].[fn_get_default_value] ('filegroup','sat')					as varchar(128))

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

select @sat_source_date_time = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Source_Date_Time'

select @sat_tombstone_indicator = quotename(column_name)
from [dbo].[dv_default_column]
where 1=1
and object_type	= 'sat'
and object_column_type = 'Tombstone_Indicator'

-- Source Table

select 	 @source_system				= s.[source_system_name]	
        ,@source_database			= s.[timevault_name]
		,@source_schema				= t.[source_table_schema]
		,@source_table				= t.[source_table_name]
		,@source_table_config_key	= t.[source_table_key]
		,@source_qualified_name		= quotename(s.[timevault_name]) + '.' + quotename(t.[source_table_schema]) + '.' + quotename(t.[source_table_name])
from [dbo].[dv_source_system] s
inner join [dbo].[dv_source_table] t
on t.system_key = s.[source_system_key]
where 1=1
and s.[source_system_name]		= @vault_source_system_name
and t.[source_table_schema]		= @vault_source_table_schema
and t.[source_table_name]		= @vault_source_table_name

-- Get Satellite Details
select 	 @sat_database			= sat.[satellite_database]						
		,@sat_schema			= coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')		
		,@sat_table				= sat.[satellite_name]		
		,@sat_surrogate_keyname	= [dbo].[fn_get_object_name] (sat.[satellite_name],'SatSurrogate')		
		,@sat_config_key		= sat.[satellite_key]		
		,@sat_link_hub_flag		= sat.[link_hub_satellite_flag]		
		,@sat_qualified_name	= quotename(sat.[satellite_database]) + '.' + quotename(coalesce(sat.[satellite_schema], @def_sat_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] (sat.[satellite_name], 'sat')))       
from [dbo].[dv_satellite] sat
where 1=1
and sat.[satellite_name] = @vault_sat_name

if @sat_link_hub_flag <> 'H' begin RAISERROR('This Check only deals with Hub Sats', 16, 1) return end;
-- Get Hub Details
select  @hub_database			= h.[hub_database]
	   ,@hub_schema				= coalesce([hub_schema], @def_hub_schema, 'dbo')				
	   ,@hub_table				= h.[hub_name]
	   ,@hub_surrogate_keyname	= (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](h.[hub_name], 'hub'))
	   ,@hub_config_key			= h.[hub_key]
	   ,@hub_qualified_name		= quotename([hub_database]) + '.' + quotename(coalesce([hub_schema], @def_hub_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([hub_name], 'hub')))	
from [dbo].[dv_satellite] s
inner join [dbo].[dv_hub] h
on s.hub_key = h.hub_key
where 1=1
and s.[satellite_key] = @sat_config_key	




set @sat_payload = ''
set @source_payload = ''

select @sat_payload += @crlf + ', h.' + quotename(hkc.hub_key_column_name) + ' AS ' + quotename('hub_' + hkc.hub_key_column_name)
      ,@source_payload += @crlf + ', ' + quotename(c.column_name) + ' AS ' + quotename('hub_' + c.column_name)
from [dbo].[dv_satellite] s
inner join [dbo].[dv_hub] h					on h.hub_key = s.hub_key
 left join [dbo].[dv_hub_key_column] hkc	on hkc.hub_key = h.hub_key
 left join [dbo].[dv_hub_column] hc			on hc.hub_key_column_key = hkc.hub_key_column_key
 left join [dbo].[dv_column] c				on c.column_key = hc.column_key 
inner join [dbo].[dv_source_table] st		on c.table_key = st.source_table_key
inner join [dbo].[dv_source_system] ss		on ss.source_system_key = st.system_key

where s.satellite_name			= @vault_sat_name
  and ss.source_system_name		= @vault_source_system_name
  and st.source_table_schema	= @vault_source_table_schema
  and st.source_table_name		= @vault_source_table_name
  and link_hub_satellite_flag	= 'H'
  and c.is_retired = 0

select @sat_payload += @crlf + ', s.' + quotename(sc.column_name) + ' AS ' + quotename('sat_' + sc.column_name)
	  ,@source_payload += @crlf + ', ' +	
		case when [dbo].[fn_build_column_definition] ('',c.[column_type],c.[column_length],c.[column_precision],c.[column_scale],c.[Collation_Name],0,0,0,0) 
		        = [dbo].[fn_build_column_definition] ('',sc.[column_type],sc.[column_length],sc.[column_precision],sc.[column_scale],sc.[Collation_Name],0,0,0,0) 
			 then quotename(c.column_name) 
			 else [dbo].[fn_build_column_definition] (c.column_name,sc.[column_type],sc.[column_length],sc.[column_precision],sc.[column_scale],sc.[Collation_Name],0,0,1,0)
		     end + ' AS ' + quotename('sat_' + c.column_name)
from [dbo].[dv_satellite] s
inner join [dbo].[dv_satellite_column] sc	on sc.satellite_key = s.satellite_key
 left join [dbo].[dv_column] c				on c.satellite_col_key = sc.satellite_col_key
inner join [dbo].[dv_source_table] st		on c.table_key = st.source_table_key
inner join [dbo].[dv_source_system] ss		on ss.source_system_key = st.system_key

where s.satellite_name			= @vault_sat_name
  and ss.source_system_name		= @vault_source_system_name
  and st.source_table_schema	= @vault_source_table_schema
  and st.source_table_name		= @vault_source_table_name
  and link_hub_satellite_flag	= 'H'
  and c.is_retired = 0

set @SQL = 'with wBaseSet as(' + @crlf + 
		   'SELECT ' + right(@source_payload, len(@source_payload) - 3) + @crlf + 
		   'FROM ' + @source_qualified_name + @crlf + 
		   'EXCEPT ' + @crlf + 
           'SELECT ' + right(@sat_payload, len(@sat_payload) - 3) + @crlf +
           'FROM ' + @sat_qualified_name + ' s' + @crlf +
		   'INNER JOIN ' + @hub_qualified_name + 'h ON h.' + @hub_surrogate_keyname + ' = s.' + @hub_surrogate_keyname + @crlf +
		   'WHERE s.' + @sat_current_row_col + ' = 1 AND s.' + @sat_tombstone_indicator  + ' = 0' + ')' + @crlf +
		   'SELECT ''' + @source_qualified_name + ''' AS SourceTable, ''' + @sat_qualified_name + ''' AS Satellite , COUNT(*) AS CountOfMissingRows FROM wBaseSet'
print @SQL
exec sp_executesql @SQL 

END