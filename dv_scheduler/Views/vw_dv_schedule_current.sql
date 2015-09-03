

CREATE view [dv_scheduler].[vw_dv_schedule_current] as 
SELECT [schedule_key]
      ,[schedule_name]
      ,[schedule_description]
      ,[schedule_frequency]
      ,[release_key]
      ,[version_number]
      ,[updated_by]
      ,[updated_datetime]
  FROM [dv_scheduler].[dv_schedule]
  where isnull([is_cancelled], 0) = 0