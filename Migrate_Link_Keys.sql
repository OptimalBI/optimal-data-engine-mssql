insert [dbo].[dv_link_key_column] ([link_key],[link_key_column_name]) (
SELECT 
	   l.link_key
	  ,h.hub_name
  FROM [dbo].[dv_hub_link_to_be_removed] hl
  inner join [dbo].[dv_hub] h on h.hub_key = hl.hub_key
  inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key = h.hub_key
  
  inner join [dbo].[dv_link] l on l.link_key = hl.[link_key])

go

update hc
  set link_key_column_key = lkc.link_key_column_key
   FROM [dbo].[dv_hub_link_to_be_removed] hl
  inner join [dbo].[dv_hub] h on h.hub_key = hl.hub_key
  inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key = h.hub_key
  inner join [dbo].[dv_hub_column] hc on hc.hub_key_column_key = hkc.hub_key_column_key
  
  inner join [dbo].[dv_link] l on l.link_key = hl.[link_key]
  inner join [dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key and lkc.link_key_column_name = h.hub_name 