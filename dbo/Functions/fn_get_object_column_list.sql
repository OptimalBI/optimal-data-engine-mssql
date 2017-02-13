

CREATE FUNCTION [dbo].[fn_get_object_column_list]
(
	@object_key int, 
	@object_type varchar(50),
	@alias varchar(50) = NULL
)
RETURNS 
@column_list TABLE 
(
	[column_name]			varchar(128)
   ,[column_qualified_name] varchar(256) NULL
   ,[column_type]			varchar(50)	 NULL
   ,[column_length]			int			 NULL
   ,[column_precision]		int			 NULL
   ,[column_scale]			int			 NULL
   ,[collation_name]		sysname		 NULL
   ,[column_definition]     varchar(128) NULL
   ,[column_cast]			varchar(512) NULL
   ,[column_object_key]     int			 NULL
)
AS
BEGIN
declare  @c_hub_key							int
		,@c_hub_name                        varchar(128)
		,@c_hub_schema						varchar(128)
		,@c_hub_database					varchar(128)
		,@c_link_key_name					varchar(128)
		,@c_link_key_column_key				int
		,@c_hub_data_type					varchar(128)
/*  -------------   Hub Logic   -------------------- */ 
if @object_type = 'hub'
	begin
	if isnull(@alias, '') = '' set @alias = 'h'
	insert @column_list
	select hkc.hub_key_column_name
	      ,quotename(@alias) + '.' + quotename(hkc.hub_key_column_name)
	      ,hkc.hub_key_column_type
		  ,hkc.hub_key_column_length
		  ,hkc.hub_key_column_precision
		  ,hkc.hub_key_column_scale
		  ,hkc.hub_key_Collation_Name		  
		  ,[dbo].[fn_build_column_definition] (hkc.hub_key_column_name, hkc.hub_key_column_type,hkc.hub_key_column_length, hub_key_column_precision, hkc.hub_key_column_scale, hkc.hub_key_Collation_Name, 0,0,0,0)
		  ,[dbo].[fn_build_column_definition] (hkc.hub_key_column_name, hkc.hub_key_column_type,hkc.hub_key_column_length, hub_key_column_precision, hkc.hub_key_column_scale, hkc.hub_key_Collation_Name, 0,0,1,1)
		  ,hkc.hub_key_column_key
		from [dbo].[dv_hub] h
		inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key = h.hub_key		
		where h.[hub_key] = @object_key
		order by hkc.[hub_key_ordinal_position]
	end
/*  -------------   Link Logic   -------------------- */   
else if @object_type = 'lnk'
	begin
	if isnull(@alias, '') = '' set @alias = 'l'
    insert @column_list
	select c.column_name
	      ,quotename(@alias) + '.' + c.column_name 
		  , c.column_type
		  , c.column_length
		  , c.column_precision
		  , c.column_scale
		  , c.collation_Name
		  ,[dbo].[fn_build_column_definition] (c.column_name, c.column_type,c.column_length, c.column_precision, c.column_scale, c.Collation_Name, 0,0,0,0)
		  ,[dbo].[fn_build_column_definition] (c.column_name, c.column_type,c.column_length, c.column_precision, c.column_scale, c.Collation_Name, 0,0,1,1)
		  ,lkc.link_key_column_key
	from [dbo].[dv_link] l
		inner join [dbo].[dv_link_key_column] lkc on lkc.[link_key_column_key] = l.[link_key]
		inner join [dbo].[dv_hub_column] hc on hc.link_key_column_key = lkc.[link_key_column_key]
		cross apply[dbo].[fn_get_key_definition] (lkc.link_key_column_name,'lnk') c
		where l.[link_key] = @object_key
		order by lkc.[link_key_column_name]
	
	end
/*  -------------   Sat Logic   -------------------- */ 
else if @object_type = 'sat'
	begin
	if isnull(@alias, '') = '' set @alias = 's'
    insert @column_list  
	select sc.column_name
	      ,quotename(@alias) + '.' + quotename(sc.column_name)
		  ,sc.column_type
		  ,sc.column_length
		  ,sc.column_precision
		  ,sc.column_precision
		  ,sc.Collation_Name
		  ,[dbo].[fn_build_column_definition] (sc.column_name, sc.column_type,sc.column_length, sc.column_precision, sc.column_scale, sc.Collation_Name, 0,0,0,0)
		  ,[dbo].[fn_build_column_definition] (sc.column_name, sc.column_type,sc.column_length, sc.column_precision, sc.column_scale, sc.Collation_Name, 0,0,1,1)
		  ,sc.satellite_col_key

	from [dbo].[dv_satellite] s
		inner join [dbo].[dv_satellite_column] sc on sc.satellite_key = s.satellite_key		
		where s.[satellite_key] = @object_key
		order by [satellite_ordinal_position]
	end

/*  -------------   Stage Logic   -------------------- */ 
else if @object_type = 'stg'
begin
	if isnull(@alias, '') = '' set @alias = 'st'
    insert @column_list
	select c.column_name
	      ,quotename(@alias) + '.' + quotename(c.column_name)
		  ,c.column_type
		  ,c.column_length
		  ,c.column_precision
		  ,c.column_precision
		  ,c.Collation_Name
		  ,[dbo].[fn_build_column_definition] (c.column_name, c.column_type,c.column_length, c.column_precision, c.column_scale, c.Collation_Name, 0,0,0,0)
		  ,[dbo].[fn_build_column_definition] (c.column_name, c.column_type,c.column_length, c.column_precision, c.column_scale, c.Collation_Name, 0,0,1,1)
		  ,c.column_key

	from [dbo].[dv_source_table] s
		inner join [dbo].[dv_column] c on c.[table_key] = s.[source_table_key]		
		where s.[source_table_key] = @object_key
		order by c.column_name
	end

RETURN 
END