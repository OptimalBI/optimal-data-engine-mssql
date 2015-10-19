create view [dv_scheduler].[vw_dv_schedule_source_table_current]
as
SELECT [schedule_source_table_key]
      ,[schedule_key]
      ,[source_table_key]
      ,[source_table_load_type]
      ,[priority]
      ,[queue]
      ,[release_key]
      ,[version_number]
      ,[updated_by]
      ,[updated_datetime]
  FROM [dv_scheduler].[dv_schedule_source_table]
  where isnull([is_cancelled], 0) = 0