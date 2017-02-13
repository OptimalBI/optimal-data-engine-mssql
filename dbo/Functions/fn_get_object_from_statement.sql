
CREATE FUNCTION [dbo].[fn_get_object_from_statement]
(
	@object_key int
   ,@object_type varchar(50)
   ,@alias varchar(50) = NULL
)
RETURNS nvarchar(max)
AS
BEGIN
	DECLARE @SQL						nvarchar(max)	= ''
	       ,@wrk_link_joins				nvarchar(max)	= ''
	       ,@link_lookup_joins			nvarchar(max)   = ''
	DECLARE @crlf						char(2)			= CHAR(13) + CHAR(10)
	DECLARE @def_schema					varchar(128)	= cast([dbo].[fn_get_default_value] ('schema',@object_type) as varchar(128))
	DECLARE @c_hub_key					int
		   ,@c_hub_name                 varchar(128)
		   ,@c_hub_schema				varchar(128)
		   ,@c_hub_database				varchar(128)
		   ,@c_link_key_name			varchar(128)
		   ,@c_link_key_column_key		int
		   ,@c_hub_data_type			varchar(128)


/* ------  Hub Logic --------------*/                
	if @object_type = 'hub'
	begin
	if isnull(@alias, '') = '' set @alias = 'h'
	select @SQL = 'FROM ' + quotename(h.hub_database) + '.' + quotename(coalesce(h.hub_schema, @def_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (h.hub_name, 'hub'))) + ' ' + quotename(@alias) 
	from [dbo].[dv_hub] h 
	where h.hub_key = @object_key 
	end

/* ------  Link Logic --------------*/    
	if @object_type = 'lnk'
	begin
	if isnull(@alias, '') = '' set @alias = 'l'
	select @SQL = 'FROM ' + quotename(l.link_database) + '.' + quotename(coalesce(l.link_schema, @def_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (l.link_name, 'lnk'))) + ' ' + quotename(@alias) 
	from [dbo].[dv_link] l
	where l.link_key = @object_key
	end

/* ------  Sat Logic --------------*/    
	if @object_type = 'sat'
	begin
	if isnull(@alias, '') = '' set @alias = 's'
	select @SQL = 'FROM ' + quotename(s.satellite_database) + '.' + quotename(coalesce(s.satellite_schema, @def_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (s.satellite_name, 'sat'))) + ' ' + quotename(@alias)  
	from [dbo].[dv_satellite] s 
	where s.satellite_key = @object_key 
	end
/* ------  Stage Logic --------------*/	
	if @object_type = 'stg'
	begin
	if isnull(@alias, '') = '' set @alias = 'st'
	select @SQL = 'FROM ' + quotename(sd.stage_database_name) + '.' + quotename(coalesce(ss.stage_schema_name, @def_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (st.stage_table_name, 'stg'))) + ' ' + quotename(@alias) 
	from [dbo].[dv_source_table] st
	inner join [dbo].[dv_stage_schema] ss on ss.stage_schema_key = st.stage_schema_key
	inner join [dbo].[dv_stage_database] sd on sd.stage_database_key = ss.stage_database_key
	where st.source_table_key = @object_key 
	end
	RETURN @SQL

END