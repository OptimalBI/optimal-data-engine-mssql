create view [dv_scheduler].[vw_dv_source_table_hierarchy_current]
as
SELECT [source_table_hierarchy_key]
      ,[source_table_key]
      ,[prior_table_key]
      ,[release_key]
      ,[version_number]
      ,[updated_by]
      ,[update_date_time]
  FROM [dv_scheduler].[dv_source_table_hierarchy]
  where isnull([is_cancelled], 0) = 0