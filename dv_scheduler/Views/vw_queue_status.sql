create view dv_scheduler.vw_queue_status
as
SELECT t1.NAME AS [Service_Name]
	,t3.NAME AS [Schema_Name]
	,t2.NAME AS [Queue_Name]
	,CASE 
		WHEN t4.STATE IS NULL
			THEN 'Not available'
		ELSE t4.STATE
		END AS [Queue_State]
	,CASE 
		WHEN t4.tasks_waiting IS NULL
			THEN '--'
		ELSE CONVERT(VARCHAR, t4.tasks_waiting)
		END AS tasks_waiting
	,CASE 
		WHEN t4.last_activated_time IS NULL
			THEN '--'
		ELSE CONVERT(VARCHAR, t4.last_activated_time)
		END AS last_activated_time
	,CASE 
		WHEN t4.last_empty_rowset_time IS NULL
			THEN '--'
		ELSE CONVERT(VARCHAR, t4.last_empty_rowset_time)
		END AS last_empty_rowset_time
	,(
		SELECT COUNT(*)
		FROM sys.transmission_queue t6
		WHERE (t6.from_service_name = t1.NAME)
		) AS [Tran_Message_Count]
FROM sys.services t1
INNER JOIN sys.service_queues t2 ON (t1.service_queue_id = t2.object_id)
INNER JOIN sys.schemas t3 ON (t2.schema_id = t3.schema_id)
LEFT JOIN sys.dm_broker_queue_monitors t4 ON (
		t2.object_id = t4.queue_id
		AND t4.database_id = DB_ID()
		)
INNER JOIN sys.databases t5 ON (t5.database_id = DB_ID())
WHERE t1.NAME NOT IN (
		'http://schemas.microsoft.com/SQL/Notifications/QueryNotificationService'
		,'http://schemas.microsoft.com/SQL/Notifications/EventNotificationService'
		,'http://schemas.microsoft.com/SQL/ServiceBroker/ServiceBroker'
		)