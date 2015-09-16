

create view [dv_scheduler].[vw_running_processes] 
as
SELECT spid
      , CAST(((DATEDIFF(s,start_time,GetDate()))/3600) as varchar) + ' hour(s), '
      + CAST((DATEDIFF(s,start_time,GetDate())%3600)/60 as varchar) + 'min, '
      + CAST((DATEDIFF(s,start_time,GetDate())%60) as varchar) + ' sec' as running_time
	  ,ER.command
	  ,ER.blocking_session_id
	  ,SP.dbid
	  ,last_wait_type = LASTWAITTYPE
	  ,db_name = DB_NAME(SP.DBID) 
	  ,SUBSTRING(est.text, (ER.statement_start_offset/2)+1
	  ,((CASE ER.statement_end_offset
					 WHEN -1 THEN DATALENGTH(est.text)
					 ELSE ER.statement_end_offset
					 END - ER.statement_start_offset)/2) + 1) as query_text 
	  ,cpu
	  ,host_name = HOSTNAME
	  ,login_time
	  ,login_name = LOGINAME
	  ,SP.status
	  ,program_name
	  ,NT_domain
	  ,NT_username
FROM SYSPROCESSES SP
INNER JOIN sys.dm_exec_requests ER
ON sp.spid = ER.session_id
CROSS APPLY SYS.DM_EXEC_SQL_TEXT(er.sql_handle) EST