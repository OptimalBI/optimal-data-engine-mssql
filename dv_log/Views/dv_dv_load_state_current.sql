create view dv_log.dv_dv_load_state_current
as
SELECT ls.load_state_key
      ,st1.stage_table_name
	  ,ls.object_type
	  ,case ls.object_type 
			when 'hub' then h.hub_name 
			when 'lnk' then l.link_name
			when 'sat' then s.satellite_name
			when 'stg' then st.stage_table_name
			end as [vault_object_name]
	  ,[load_high_water]
	  ,[lookup_start_datetime]
	  ,[load_start_datetime]
	  ,[load_end_datetime]
	  ,[rows_inserted]
	  ,[rows_updated]
	  ,[rows_deleted]
	  ,[rows_affected]
  FROM [dv_log].[dv_load_state] ls
  LEFT JOIN [dbo].[dv_hub] h			on h.hub_key		= ls.object_key and ls.object_type = 'hub'
  LEFT JOIN [dbo].[dv_link] l			on l.link_key		= ls.object_key and ls.object_type = 'lnk'
  LEFT JOIN [dbo].[dv_satellite] s		on s.satellite_key	= ls.object_key and ls.object_type = 'sat'
  LEFT JOIN [dbo].[dv_source_table] st	on st.source_table_key	= ls.object_key and ls.object_type = 'stg'
  LEFT JOIN [dbo].[dv_source_table] st1	on st1.source_table_key	= ls.source_table_key