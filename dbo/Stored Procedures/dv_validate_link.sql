CREATE PROCEDURE [dbo].[dv_validate_link]
(
  @vault_source_system_name		varchar(128) = NULL
, @vault_source_table_schema	varchar(128) = NULL
, @vault_source_table_name		varchar(128) = NULL
, @vault_link_name				varchar(128) = NULL
)
AS
BEGIN
SET NOCOUNT ON
 /*
	 select * from [dbo].[dv_source_system]
	 select * from [dbo].[dv_source_table]
	 select * from [dbo].[dv_link]
*/
 --declare
	-- @vault_source_system_name				varchar(128) = 'ODE'
	--,@vault_source_table_schema				varchar(128) = 'stage'
	--,@vault_source_table_name				varchar(128) = 'link_Sale'
	--,@vault_link_name						varchar(128) = 'Sale'


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
-- Link Defaults
                ,@def_link_prefix                       varchar(128)
                ,@def_link_schema                       varchar(128)
                ,@def_link_filegroup                    varchar(128)

declare  @c_hub_key							int
		,@c_hub_name                        varchar(128)
		,@c_hub_schema						varchar(128)
		,@c_hub_database					varchar(128)
		,@c_link_key_name					varchar(128)
		,@c_link_key_column_key				int
		,@c_hub_data_type					varchar(128)
-- Source Table
		,@source_table_config_key			int	
		,@source_qualified_name				varchar(512)

-- Link Table
        ,@link_database                     varchar(128)
        ,@link_schema                       varchar(128)
        ,@link_table                        varchar(128)
        ,@link_surrogate_keyname            varchar(128)
        ,@link_config_key                   int
		,@link_qualified_name				varchar(512)

DECLARE  @wrk_link_joins					nvarchar(max)
	    ,@wrk_hub_column_keys				nvarchar(max)
	    ,@wrk_src_column_keys				nvarchar(max)
        ,@link_hub_keys						nvarchar(max)
	    ,@link_lookup_joins					nvarchar(max)
		,@SQL								nvarchar(max)	


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

-- Source Table
select   @source_table_config_key				= t.[source_table_key]
        ,@source_qualified_name					= quotename(s.[timevault_name]) + '.' + quotename(t.[source_table_schema]) + '.' + quotename(t.[source_table_name])
from [dbo].[dv_source_system] s			
inner join [dbo].[dv_source_table] t			on t.system_key = s.[source_system_key]
where 1=1
and s.[source_system_name]						= @vault_source_system_name
and t.[source_table_schema]						= @vault_source_table_schema
and t.[source_table_name]						= @vault_source_table_name

-- Link
 select   @link_database						= l.[link_database]
         ,@link_schema							= coalesce(l.[link_schema], @def_link_schema, 'dbo')
         ,@link_table							= l.[link_name]
		 ,@link_surrogate_keyname				= (select replace(replace(column_name, '[', ''), ']', '') from [dbo].[fn_get_key_definition](l.[link_name], 'lnk'))
         ,@link_config_key						= l.[link_key]
         ,@link_qualified_name					= quotename([link_database]) + '.' + quotename(coalesce(l.[link_schema], @def_link_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] ([link_name], 'lnk')))
from [dbo].[dv_link] l
where 1=1
and l.link_name = @vault_link_name

set @link_hub_keys			= ''
set @wrk_src_column_keys	= ''
set @link_lookup_joins		= ''
set @wrk_hub_column_keys	= ''

-- Loop through all Hubs for the Link
DECLARE c_hub_key CURSOR FOR
select distinct 
       h.[hub_key]
	  ,h.[hub_name]  
	  ,h.[hub_schema]
	  ,h.[hub_database]
	  ,[link_key_name] = isnull(lkc.[link_key_column_name],h.[hub_name])
	  ,lkc.link_key_column_key 
	  ,data_type = rtrim([dbo].[fn_build_column_definition] ('',hkc.[hub_key_column_type],hkc.[hub_key_column_length],hkc.[hub_key_column_precision],hkc.[hub_key_column_scale],hkc.[hub_key_Collation_Name],0,0,0,0)) 
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
	,@c_hub_data_type

WHILE @@FETCH_STATUS = 0
BEGIN 

		select @wrk_link_joins  = 'INNER JOIN ' + quotename(@c_hub_database) + '.' + quotename(coalesce(@c_hub_schema, @def_hub_schema, 'dbo')) + '.' + quotename((select [dbo].[fn_get_object_name] (@c_hub_name, 'hub'))) + ' ' + @c_link_key_name + @crlf + ' ON  ' +
		                          @c_link_key_name + '.' + (select column_name from [dbo].[fn_get_key_definition](@c_hub_name, 'hub')) +
			                     ' =  l.' + (select column_name from [dbo].[fn_get_key_definition](@c_link_key_name, 'hub'))
										+ @crlf + ' AND '	
		select @wrk_src_column_keys   += ', ' + quotename([column_name]) + 
		                                 case when @c_hub_data_type <> col_data_type 
											  then ' = ' + hub_data_type_cast
										      else '' 
										      end 
											  + @crlf
		      ,@wrk_hub_column_keys += ', ' + @c_link_key_name + '.' + [hub_key_column_name] + ' AS ' + 
								@c_link_key_name + [hub_key_column_name] + @crlf    
		from (
		 select distinct 
	     h.[hub_name]
		,col_data_type = [dbo].[fn_build_column_definition] ('',c.[column_type],c.[column_length],c.[column_precision],c.[column_scale],c.[Collation_Name],0,0,0,0)
		,hub_data_type_cast = [dbo].[fn_build_column_definition] (quotename([column_name]),hkc.[hub_key_column_type],hkc.[hub_key_column_length],hkc.[hub_key_column_precision],hkc.[hub_key_column_scale],hkc.[hub_key_Collation_Name],0,0,1,0)
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
		and st.[source_table_key] = @source_table_config_key
		and c.is_retired <> 1) hkc
		ORDER BY hkc.hub_key_ordinal_position
		--print @wrk_src_column_keys
		-------------------

		set @link_lookup_joins = @link_lookup_joins + left(@wrk_link_joins, len(@wrk_link_joins) - 4)
		FETCH NEXT FROM c_hub_key
		INTO @c_hub_key
				,@c_hub_name
				,@c_hub_schema
				,@c_hub_database
				,@c_link_key_name
				,@c_link_key_column_key
				,@c_hub_data_type
END

	CLOSE c_hub_key
	DEALLOCATE c_hub_key

set @SQL = 'with wBaseSet as(' + @crlf +
		   'SELECT ' + @crlf + '  ' +
		   right(@wrk_src_column_keys, len(@wrk_src_column_keys) -2) + @crlf +
		   'FROM ' + @source_qualified_name + @crlf +
		   'EXCEPT ' + @crlf +
           'SELECT ' + @crlf + '  ' + 
		   right(@wrk_hub_column_keys, len(@wrk_hub_column_keys) -2) + @crlf +
           'FROM ' + @link_qualified_name + ' l' + @crlf +
		   @link_lookup_joins + ')' + @crlf +
		   'SELECT ''' + @source_qualified_name + ''' AS SourceTable, ''' + @link_qualified_name + ''' AS Link , COUNT(*) AS CountOfMissingRows FROM wBaseSet'

print @SQL
exec sp_executesql @SQL 
 
END