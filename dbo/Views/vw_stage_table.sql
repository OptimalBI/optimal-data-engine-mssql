

CREATE view [dbo].[vw_stage_table] as
select st.[source_table_key]
      ,st.[source_unique_name]
      ,sv.[source_type]
      ,st.[load_type]
      ,st.[system_key]
      ,[source_schema] = st.[source_table_schma]
      ,[source_name] = st.[source_table_nme]
      ,st.[stage_table_name]
	  ,ss.[stage_schema_key]
      ,[stage_schema] =ss.[stage_schema_name]
	  ,sd.[stage_database_key]
	  ,[stage_database] = sd.[stage_database_name]
from [dbo].[dv_source_table] st
inner join [dbo].[dv_source_version] sv on sv.source_table_key = st.source_table_key
inner join [dbo].[dv_stage_schema] ss	on ss.stage_schema_key = st.stage_schema_key
inner join [dbo].[dv_stage_database] sd on sd.stage_database_key = ss.stage_database_key

where (st.[is_retired] = 0 and ss.[is_retired] = 0 and sd.[is_retired] = 0)