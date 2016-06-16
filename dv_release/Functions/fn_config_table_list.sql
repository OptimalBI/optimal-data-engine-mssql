CREATE FUNCTION [dv_release].[fn_config_table_list]()
RETURNS TABLE 
AS
RETURN 
(
      SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_default_column'			as sysname), dv_key_name = cast('default_column_key'		as sysname), dv_load_order = cast(10  as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_defaults'					as sysname), dv_key_name = cast('default_key'				as sysname), dv_load_order = cast(20  as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_source_system'			as sysname), dv_key_name = cast('source_system_key'			as sysname), dv_load_order = cast(30  as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_source_table'				as sysname), dv_key_name = cast('source_table_key'			as sysname), dv_load_order = cast(40  as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_column'					as sysname), dv_key_name = cast('column_key'				as sysname), dv_load_order = cast(50  as int)
union SELECT dv_schema_name = cast('dv_scheduler'	as sysname), dv_table_name = cast('dv_schedule'					as sysname), dv_key_name = cast('schedule_key'				as sysname), dv_load_order = cast(60  as int)
union SELECT dv_schema_name = cast('dv_scheduler'	as sysname), dv_table_name = cast('dv_source_table_hierarchy'	as sysname), dv_key_name = cast('source_table_hierarchy_key'as sysname), dv_load_order = cast(70  as int)
union SELECT dv_schema_name = cast('dv_scheduler'	as sysname), dv_table_name = cast('dv_schedule_source_table'	as sysname), dv_key_name = cast('schedule_source_table_key'	as sysname), dv_load_order = cast(80  as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_column_relationship'		as sysname), dv_key_name = cast('column_relationship_key'	as sysname), dv_load_order = cast(90  as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_hub'						as sysname), dv_key_name = cast('hub_key'					as sysname), dv_load_order = cast(100 as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_hub_key_column'			as sysname), dv_key_name = cast('hub_key_column_key'		as sysname), dv_load_order = cast(110 as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_hub_column'				as sysname), dv_key_name = cast('hub_col_key'				as sysname), dv_load_order = cast(120 as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_link'						as sysname), dv_key_name = cast('link_key'					as sysname), dv_load_order = cast(130 as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_hub_link'					as sysname), dv_key_name = cast('hub_link_key'				as sysname), dv_load_order = cast(140 as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_satellite'				as sysname), dv_key_name = cast('satellite_key'				as sysname), dv_load_order = cast(150 as int)
union SELECT dv_schema_name = cast('dbo'			as sysname), dv_table_name = cast('dv_satellite_column'			as sysname), dv_key_name = cast('satellite_col_key'			as sysname), dv_load_order = cast(160 as int)
)