




CREATE view [dbo].[vw_LR_match_config_details] as 
select
st.[source_table_key],
sv.[source_version_key], 
left_object_name			 = coalesce(l_h.hub_name	, l_l.link_name		, l_s.satellite_name	,l_st.stage_table_name, ''),		
left_object_schema			 = coalesce(l_h.hub_schema	, l_l.link_schema	, l_s.satellite_schema	,l_st.stage_schema, ''),		
left_object_database		 = coalesce(l_h.hub_database, l_l.link_database	, l_s.satellite_database,l_st.stage_database, ''),			
left_object_type			 = case 
								when l_h.hub_key is not null			then 'hub'
								when l_l.link_key is not null			then 'lnk'		
								when l_s.satellite_key is not null		then 'sat'	
								when l_st.source_table_key is not null	then 'stg'
								else null
								end,			
left_sat_pit				= case when l_s.satellite_key is not null then om.[temporal_pit_left] else NULL end, 		
left_object_filter			= NULL,	
right_object_name			= coalesce(r_h.hub_name	, r_l.link_name		, r_s.satellite_name	,r_st.stage_table_name, ''),
right_object_schema			= coalesce(r_h.hub_schema, r_l.link_schema	, r_s.satellite_schema	,r_st.stage_schema, ''),	
right_object_database		= coalesce(r_h.hub_database, r_l.link_database	, r_s.satellite_database,r_st.stage_database, ''),
right_object_type			= case 
								when r_h.hub_key is not null			then 'hub'
								when r_l.link_key is not null			then 'lnk'		
								when r_s.satellite_key is not null		then 'sat'	
								when r_st.source_table_key is not null	then 'stg'
								else null
								end,	
right_sat_pit				= case when r_s.satellite_key is not null then om.[temporal_pit_right] else NULL end,		
right_object_filter			= NULL,	
output_database				= st.stage_database,		
output_schema				= st.stage_schema,			
output_name					= st.stage_table_name,			
match_key					= om.match_key,
left_column_name			= case 
								when l_h.hub_key is not null			then l_hkc.hub_key_column_name					--'hub'
								when l_l.link_key is not null			then l_l_hkc.hub_key_column_name					-- 'lnk'		
								when l_s.satellite_key is not null		then l_sc.column_name							-- 'sat'	
								when l_st.source_table_key is not null	then l_c.column_name							--stg
								else null
								end,
			
right_column_name		    = case 
								when r_h.hub_key is not null			then r_hkc.hub_key_column_name					--'hub'
								when r_l.link_key is not null			then r_l_hkc.hub_key_column_name				-- 'lnk'		
								when r_s.satellite_key is not null		then r_sc.column_name							-- 'sat'	
								when r_st.source_table_key is not null	then r_c.column_name							--stg
								else null
								end	
from [dbo].[vw_stage_table] st
inner join [dbo].[dv_source_version] sv on sv.source_table_key = st.source_table_key 
									   and sv.is_current = 1
inner join [dbo].[dv_object_match] om on om.source_version_key = sv.source_version_key
inner join [dbo].[dv_column_match] cm on cm.match_key = om.match_key

left join [dbo].[dv_hub_key_column]   l_hkc on l_hkc.hub_key_column_key		= cm.[left_hub_key_column_key]
left join [dbo].[dv_hub]              l_h   on l_h.hub_key					= l_hkc.hub_key

left join [dbo].[dv_link_key_column]  l_lkc on l_lkc.link_key_column_key	= cm.[left_link_key_column_key]
left join [dbo].[dv_link]			  l_l   on l_l.link_key					= l_lkc.link_key
left join [dbo].[dv_hub_column]		  l_hc  on l_hc.[link_key_column_key]	= cm.[left_link_key_column_key]
left join [dbo].[dv_hub_key_column]   l_l_hkc on l_l_hkc.[hub_key_column_key] = l_hc.[hub_key_column_key]


left join [dbo].[dv_satellite_column] l_sc  on l_sc.satellite_col_key		= cm.[left_satellite_col_key]
left join [dbo].[dv_satellite]		  l_s   on l_s.satellite_key			= l_sc.satellite_key

left join [dbo].[dv_column]           l_c   on l_c.column_key				= cm.[left_column_key]
left join [dbo].[vw_stage_table]      l_st  on l_st.source_table_key		= l_c.table_key

left join [dbo].[dv_hub_key_column]   r_hkc on r_hkc.hub_key_column_key		= cm.[right_hub_key_column_key]
left join [dbo].[dv_hub]              r_h   on r_h.hub_key					= r_hkc.hub_key

left join [dbo].[dv_link_key_column]  r_lkc on r_lkc.link_key_column_key	= cm.[right_link_key_column_key]
left join [dbo].[dv_link]			  r_l   on r_l.link_key					= r_lkc.link_key
left join [dbo].[dv_hub_column]		  r_hc  on r_hc.[link_key_column_key]	= cm.[right_link_key_column_key]
left join [dbo].[dv_hub_key_column]   r_l_hkc on r_l_hkc.[hub_key_column_key] = r_hc.[hub_key_column_key]

left join [dbo].[dv_satellite_column] r_sc  on r_sc.satellite_col_key		= cm.[right_satellite_col_key]
left join [dbo].[dv_satellite]		  r_s   on r_s.satellite_key			= r_sc.satellite_key

left join [dbo].[dv_column]           r_c   on r_c.column_key				= cm.[right_column_key]
left join [dbo].[vw_stage_table]      r_st  on r_st.source_table_key		= r_c.table_key

where st.source_type = 'LeftRightComparison'