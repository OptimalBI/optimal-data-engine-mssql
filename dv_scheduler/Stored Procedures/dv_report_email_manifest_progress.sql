
CREATE PROCEDURE [dv_scheduler].[dv_report_email_manifest_progress] (
	@vault_offset_in_days INT = 1
	,@vault_recipients VARCHAR(max)
	,@vault_profile_name VARCHAR(128)
	,@vault_output_results BIT = 1
	,@vault_email_results BIT = 0
	,@vault_html_string VARCHAR(max) = ' ' OUTPUT
	)
AS
BEGIN
	SET NOCOUNT ON

	IF EXISTS (SELECT 1 FROM tempdb.dbo.sysobjects WHERE id = object_id(N'tempdb..#RecentExecutions')) DROP TABLE #RecentExecutions
	IF EXISTS (SELECT 1 FROM tempdb.dbo.sysobjects WHERE id = object_id(N'tempdb..#RecentErrors')) DROP TABLE #RecentErrors
	IF EXISTS (SELECT 1 FROM tempdb.dbo.sysobjects WHERE id = object_id(N'tempdb..#Log4Exceptions')) DROP TABLE #Log4Exceptions

			/******************************************************************************************************/
			/******************************************************************************************************/
			/*                            Table for List of Recent Executions:                                    */
			/******************************************************************************************************/
			/******************************************************************************************************/
			;

	WITH wBaseSet
	AS (
		SELECT [run_schedule_name]
			,[run_key]				= cast([run_key] AS VARCHAR)
			,[run_status]
			,[run_start_datetime]	= format([run_start_datetime], 'yyyy/MM/dd HH:mm:ss')
			,[run_end_datetime]		= format([run_end_datetime], 'yyyy/MM/dd HH:mm:ss')
			,[run_duration]			= convert(VARCHAR, [run_duration], 108)
			,[run_manifest_status]
			,[task_count]			= count(*)
		FROM [dv_scheduler].[vw_manifest_status]
		WHERE 1 = 1
			AND [run_start_datetime] >= dateadd(day, @vault_offset_in_days * - 1, sysdatetimeoffset())
		GROUP BY [run_schedule_name]
			,[run_key]
			,[run_status]
			,[run_start_datetime]
			,[run_end_datetime]
			,[run_duration]
			,[run_manifest_status]
		)
		,wNumbered
	AS (
		SELECT [run_schedule_name]
			,[run_key]
			,[run_status]
			,[run_start_datetime]
			,[run_end_datetime]
			,[run_duration]
			,[run_manifest_status]
			,[task_count]
			,[rn] = row_number() OVER (
				ORDER BY [run_key]
					,[run_manifest_status]
				)
		FROM wBaseSet
		)
		,cte
	AS (
		SELECT [run_schedule_name]
			,[run_key]
			,[run_status]
			,[run_start_datetime]
			,[run_end_datetime]
			,[run_duration]
			,[run_manifest_status]	= min([run_manifest_status])
			,[task_count]			= min([task_count])
			,[rn]					= min([rn])
		FROM wNumbered
		GROUP BY [run_schedule_name]
			,[run_key]
			,[run_status]
			,[run_start_datetime]
			,[run_end_datetime]
			,[run_duration]
		)
	SELECT [run_schedule_name]	= isnull(c.[run_schedule_name], ' ')
		,[run_key]				= isnull(c.[run_key], ' ')
		,[run_status]			= isnull(c.[run_status], ' ')
		,[run_start_datetime]	= isnull(c.[run_start_datetime], ' ')
		,[run_end_datetime]		= isnull(c.[run_end_datetime], ' ')
		,[run_duration]			= isnull(c.[run_duration], ' ')
		,[run_manifest_status]	= isnull(t.[run_manifest_status], ' ')
		,[task_count]			= isnull(t.[task_count], ' ')
		,[row_number]			= t.[rn]
		,[TRRow]				= rank() OVER (ORDER BY t.[run_key]) % 2
	INTO #RecentExecutions
	FROM wNumbered t
	LEFT JOIN cte c ON t.[rn] = c.[rn]
	ORDER BY t.[rn]
		/******************************************************************************************************/
		/******************************************************************************************************/
		/*                            Table for List of Recent Tasks in Error:                                */
		/******************************************************************************************************/
		/******************************************************************************************************/
		;

	WITH wBaseSet
	AS (
		SELECT [run_schedule_name]
			,[run_key]				= cast([run_key] AS VARCHAR)
			,[source_table_name]
			,[start_datetime]		= format([start_datetime], 'yyyy/MM/dd HH:mm:ss')
			,[completed_datetime]	= format(isnull([completed_datetime], [run_end_datetime]), 'yyyy/MM/dd HH:mm:ss')
			,[run_manifest_status]
			,[session_id]			= cast([session_id] AS VARCHAR)
		FROM [dv_scheduler].[vw_manifest_status]
		WHERE 1 = 1
			AND [run_start_datetime] >= dateadd(day, @vault_offset_in_days * - 1, sysdatetimeoffset())
			AND [run_manifest_status] <> 'Completed'
		)
		,wNumbered
	AS (
		SELECT [run_schedule_name]
			,[run_key]
			,[source_table_name]
			,[start_datetime]
			,[completed_datetime]
			,[run_manifest_status]
			,[session_id]
			,[rn] = row_number() OVER (
				ORDER BY [run_key]
					,CASE 
						WHEN isnull([run_manifest_status], '') = 'Failed'
							THEN 1
						WHEN isnull([run_manifest_status], '') = 'Cancelled'
							THEN 2
						WHEN isnull([run_manifest_status], '') = 'Processing'
							THEN 3
						WHEN isnull([run_manifest_status], '') = 'Queued'
							THEN 4
						WHEN isnull([run_manifest_status], '') = 'Scheduled'
							THEN 5
						ELSE 99
						END
					,[source_table_name]
				)
		FROM wBaseSet
		)
		,cte
	AS (
		SELECT [run_schedule_name]
			,[run_key]
			,[source_table_name]
			,[start_datetime] = min([start_datetime])
			,[completed_datetime] = min([completed_datetime])
			,[run_manifest_status] = min([run_manifest_status])
			,[session_id] = min([session_id])
			,[rn] = min([rn])
		FROM wNumbered
		GROUP BY [run_schedule_name]
			,[run_key]
			,[source_table_name]
		)
	SELECT [run_schedule_name]	= isnull(c.[run_schedule_name], ' ')
		,[run_key]				= isnull(c.[run_key], ' ')
		,[source_table_name]	= isnull(c.[source_table_name], ' ')
		,[start_datetime]		= isnull(c.[start_datetime], '<Not Started>')
		,[completed_datetime]	= isnull(c.[completed_datetime], ' ')
		,[run_manifest_status]	= isnull(t.[run_manifest_status], ' ')
		,[session_id]			= isnull(t.[session_id], ' ')
		,[row_number]			= t.[rn]
		,[TRRow]				= rank() OVER (ORDER BY t.[rn]) % 2
	INTO #RecentErrors
	FROM wNumbered t
	LEFT JOIN cte c ON t.[rn] = c.[rn]
	ORDER BY t.[rn]
		/******************************************************************************************************/
		/******************************************************************************************************/
		/*                            Table for List of Recently (Log4) Logged Errors:                        */
		/******************************************************************************************************/
		/******************************************************************************************************/
		;

	WITH wBaseSet
	AS (
		SELECT [SessionId]		= cast(e.[SessionId] AS VARCHAR)
			,[ExceptionId]		= cast(e.[ExceptionId] AS VARCHAR)
			,[SessionLoginTime] = format(e.[SessionLoginTime], 'yyyy/MM/dd HH:mm:ss')
			,[SystemDate]		= format(e.[SystemDate], 'yyyy/MM/dd HH:mm:ss')
			,[ErrorContext]
			,[ErrorMessage]
			,[ServerName]
			,[DatabaseName]
			,[LoginName]
			,[ErrorNumber]		= cast(e.[ErrorNumber] AS VARCHAR)
			,[ErrorSeverity]	= cast(e.[ErrorSeverity] AS VARCHAR)
			,[ErrorState]		= cast(e.[ErrorState] AS VARCHAR)
			,[ErrorProcedure]
			,[ErrorLine]		= cast(e.[ErrorLine] AS VARCHAR)
			,[UtcDate]			= format(e.[UtcDate], 'yyyy/MM/dd HH:mm:ss')
			,[SourceTableName]	= t.[source_table_name]
			,[RunKey]			= cast(t.[run_key] AS VARCHAR)
			,[RunScheduleName]	= t.[run_schedule_name]
		FROM [log4].[Exception] e
		LEFT JOIN #RecentErrors t ON e.SessionId = t.session_id
			AND e.SystemDate BETWEEN CASE 
						WHEN isnull(t.start_datetime, '') = ''
							THEN '31 Dec 9999'
						ELSE t.start_datetime
						END
				AND CASE 
						WHEN isnull(t.completed_datetime, '<Not Started>') = ''
							THEN '31 Dec 9999'
						ELSE t.completed_datetime
						END
			AND t.run_manifest_status = 'Failed'
		WHERE 1 = 1
			AND e.[SystemDate] >= dateadd(day, @vault_offset_in_days * - 1, sysdatetime())
		)
		,wNumbered
	AS (
		SELECT [SessionId]
			,[ExceptionId]
			,[SessionLoginTime]
			,[SystemDate]
			,[ErrorContext]
			,[ErrorMessage]
			,[ServerName]
			,[DatabaseName]
			,[LoginName]
			,[ErrorNumber]
			,[ErrorSeverity]
			,[ErrorState]
			,[ErrorProcedure]
			,[ErrorLine]
			,[UtcDate]
			,[SourceTableName]
			,[RunKey]
			,[RunScheduleName]
			,[rn] = row_number() OVER (
				ORDER BY [SessionLoginTime]
					,[SessionId]
					,[SystemDate]
				)
		FROM wBaseSet
		)
		,cte
	AS (
		SELECT [SessionId]
			,[SessionLoginTime]
			,[ExceptionId]
			,[SystemDate]		= min([SystemDate])
			,[ErrorContext]		= min([ErrorContext])
			,[ErrorMessage]		= min([ErrorMessage])
			,[LoginName]		= min([LoginName])
			,[ErrorNumber]		= min([ErrorNumber])
			,[ErrorSeverity]	= min([ErrorSeverity])
			,[ErrorState]		= min([ErrorState])
			,[ErrorProcedure]	= min([ErrorProcedure])
			,[ErrorLine]		= min([ErrorLine])
			,[UtcDate]			= min([UtcDate])
			,[SourceTableName]	= min([SourceTableName])
			,[RunKey]			= min([RunKey])
			,[RunScheduleName]	= min([RunScheduleName])
			,[rn]				= min([rn])
		FROM wNumbered
		GROUP BY [SessionId]
			,[SessionLoginTime]
			,[ExceptionId]
		)
	SELECT [SessionId]			= isnull(c.[SessionId], ' ')
		,[SessionLoginTime]		= isnull(c.[SessionLoginTime], ' ')
		,[SystemDate]			= isnull(t.[SystemDate], ' ')
		,[ErrorContext]			= isnull(t.[ErrorContext], ' ')
		,[ErrorMessage]			= isnull(t.[ErrorMessage], ' ')
		,[LoginName]			= isnull(t.[LoginName], ' ')
		,[ErrorNumber]			= isnull(t.[ErrorNumber], ' ')
		,[ErrorSeverity]		= isnull(t.[ErrorSeverity], ' ')
		,[ErrorState]			= isnull(t.[ErrorState], ' ')
		,[ErrorProcedure]		= isnull(t.[ErrorProcedure], '')
		,[ErrorLine]			= isnull(t.[ErrorLine], ' ')
		,[UtcDate]				= isnull(t.[UtcDate], '')
		,[SourceTableName]		= isnull(t.[SourceTableName], '')
		,[RunKey]				= isnull(t.[RunKey], '')
		,[RunScheduleName]		= isnull(t.[RunScheduleName], '')
		,[row_number]			= t.[rn]
		,[TRRow]				= rank() OVER (ORDER BY t.[SessionLoginTime] DESC,t.[SessionId]) % 2
	INTO #Log4Exceptions
	FROM wNumbered t
	LEFT JOIN cte c ON t.[rn] = c.[rn]
	ORDER BY t.[rn]

	IF @vault_output_results = 1
	BEGIN
		SELECT * FROM #RecentExecutions

		SELECT * FROM #RecentErrors

		SELECT * FROM #Log4Exceptions
	END

	/******************************************************************************************************/
	/******************************************************************************************************/
	/*                            Build an HTML String                                                                                */
	/******************************************************************************************************/
	/******************************************************************************************************/
	DECLARE @Message VARCHAR(max)
		,@Subject NVARCHAR(255)
		,@Body VARCHAR(max)
		,@TableHead VARCHAR(max)
		,@TableTail VARCHAR(max)
		,@HTMLHead VARCHAR(max)
		,@HTMLTail VARCHAR(max)
		,@StartDateChar VARCHAR(50)

	SET @subject = 'ODE Progress Report Sent from ' + @@Servername

	SELECT @StartDateChar = format(dateadd(day, @vault_offset_in_days * - 1, sysdatetime()), 'yyyy/MM/dd HH:mm')

	-- HTML Header and Trailer setup
	SET @HTMLHead = '<html>' + '<head>' + '<style>' + 'td {border: 1px solid #aabcfe;padding-left:5px;padding-right:5px;padding-top:1px;padding-bottom:1px;font-size:11pt;} ' + '</style>' + '</head>' + '<body>' + '<H1><font color="Red">' + @Subject + '</H1>'
	SET @HTMLTail = '</body></html>'
	SET @TableTail = '</table>'
	SET @Message = @HTMLHead
	/******************************************************************************************************/
	/******************************************************************************************************/
	/*                            Table for List of Recent Executions:                                    */
	/******************************************************************************************************/
	/******************************************************************************************************/
	SET @TableHead = '<H3><font color="Red">ODE Scheduled Executions Since ' + @StartDateChar + ' </H3>' 
	                  + '<table cellpadding=0 cellspacing=0 border=0>' 
						+ '<tr bgcolor=#aabcfe>' 
							+ '<td align=center><b>Schedule Name</b></td>' 
							+ '<td align=right><b>Run Key</b></td>' 
							+ '<td align=center><b>Run Status</b></td>' 
							+ '<td align=center><b>Run Start</b></td>' 
							+ '<td align=center><b>Run End</b></td>' 
							+ '<td align=center><b>Run Duration</b></td>' 
							+ '<td align=center><b>Task Status</b></td>' 
							+ '<td align=right><b>Task Status Count</b></td>' 
						+ '</tr>';

	SELECT @Body = (
			SELECT [TRRow]			= rank() OVER (ORDER BY [run_key]) % 2
				,[TD]				= isnull([run_schedule_name], ' ')
				,[TD align = right] = isnull([run_key], ' ')
				,[TD]				= isnull([run_status], ' ')
				,[TD]				= isnull([run_start_datetime], ' ')
				,[TD]				= isnull([run_end_datetime], ' ')
				,[TD]				= isnull([run_duration], ' ')
				,[TD align = center]= isnull([run_manifest_status], ' ')
				,[TD align = right] = isnull([task_count], ' ')
			FROM #RecentExecutions
			ORDER BY [row_number]
			FOR XML raw('tr')
				,Elements
			)

	-- Replace the entity codes and row numbers
	SET @Body = Replace(@Body, '_x0020_', space(1))
	SET @Body = Replace(@Body, '_x003D_', '=')
	SET @Body = Replace(@Body, '<tr><TRRow>0</TRRow>', '<tr bgcolor=Gainsboro>')
	SET @Body = Replace(@Body, '<tr><TRRow>1</TRRow>', '<tr bgcolor=Snow>')
	SET @Body = Replace(@Body, '<TD align = center>Failed</TD align = center>', '<td align = center bgcolor=red>Failed</TD align = center>')

	IF @Body IS NULL
		SELECT @Body = '<H3><font color="Red">No ODE Scheduled Executions Since ' + @StartDateChar + ' </H3>'
	ELSE
		SELECT @Body = @TableHead + @Body + @TableTail

	SET @Message = @Message + @Body
	/******************************************************************************************************/
	/******************************************************************************************************/
	/*                            Table for List of Recent Tasks in Error:                                */
	/******************************************************************************************************/
	/******************************************************************************************************/
	SET @TableHead = '<H3><font color="Red">ODE Tasks Not Completed Since ' + @StartDateChar + ' </H3>' 
						+ '<table cellpadding=0 cellspacing=0 border=0>' 
							+ '<tr bgcolor=#aabcfe>'
								+ '<td align=center><b>Schedule Name</b></td>' 
								+ '<td align=right><b>Run Key</b></td>' 
								+ '<td align=center><b>Source Table</b></td>' 
								+ '<td align=center><b>Run Start</b></td>' 
								+ '<td align=center><b>Task Status</b></td>' 
								+ '<td align=right><b>Session ID</b></td>' 
							+ '</tr>';

	SELECT @Body = (
			SELECT [TRRow] = rank() OVER (
					ORDER BY [row_number]
					) % 2
				,[TD] = isnull([run_schedule_name], ' ')
				,[TD align = right] = isnull([run_key], ' ')
				,[TD] = isnull([source_table_name], ' ')
				,[TD align = center] = isnull([start_datetime], '<Not Started>')
				,[TD align = center] = isnull([run_manifest_status], ' ')
				,[TD align = right] = isnull([session_id], ' ')
			FROM #RecentErrors
			ORDER BY [row_number]
			FOR XML raw('tr')
				,Elements
			)

	-- Replace the entity codes and row numbers
	SET @Body = Replace(@Body, '_x0020_', space(1))
	SET @Body = Replace(@Body, '_x003D_', '=')
	SET @Body = Replace(@Body, '<tr><TRRow>0</TRRow>', '<tr bgcolor=Gainsboro>')
	SET @Body = Replace(@Body, '<tr><TRRow>1</TRRow>', '<tr bgcolor=Snow>')
	SET @Body = Replace(@Body, '<TD align = center>Failed</TD align = center>', '<td align = center bgcolor=red>Failed</TD align = center>')

	IF @Body IS NULL
		SELECT @Body = '<H3><font color="Red"> No ODE Tasks Not Completed Since ' + @StartDateChar + ' </H3>'
	ELSE
		SELECT @Body = @TableHead + @Body + @TableTail

	SET @Message = @Message + @Body
	/******************************************************************************************************/
	/******************************************************************************************************/
	/*                            Table for List of Recently (Log4) Logged Errors:                        */
	/******************************************************************************************************/
	/******************************************************************************************************/
	SET @TableHead = '<H3><font color="Red">ODE Errors Raised Since ' + @StartDateChar + ' </H3>' 
						+ '<table cellpadding=0 cellspacing=0 border=0>' 
							+ '<tr bgcolor=#aabcfe><td align=center><b>Session ID</b></td>' 
								+ '<td align=right><b>Session Login Time</b></td>' 
								+ '<td align=center><b>System Date</b></td>' 
								+ '<td align=center><b>Error Context</b></td>'
								+ '<td align=right><b>Error Message</b></td>' 
								+ '<td align=right><b>Login Name</b></td>' 
								+ '<td align=center><b>Error Number</b></td>' 
								+ '<td align=center><b>Error Severity</b></td>' 
								+ '<td align=right><b>Error State</b></td>' 
								+ '<td align=center><b>Error Procedure</b></td>' 
								+ '<td align=center><b>Error Line</b></td>' 
								+ '<td align=center><b>Load Task</b></td>' 
								+ '<td align=center><b>Run Key</b></td>' 
								+ '<td align=right><b>Schedule</b></td>' 
							+ '</tr>';

	SELECT @Body = (
			SELECT [TRRow]			= rank() OVER (ORDER BY [SessionLoginTime] DESC,[SessionId]) % 2
				,[TD align = right] = isnull([SessionId], ' ')
				,[TD]				= isnull([SessionLoginTime], ' ')
				,[TD]				= isnull([SystemDate], ' ')
				,[TD]				= isnull([ErrorContext], ' ')
				,[TD]				= isnull([ErrorMessage], ' ')
				,[TD]				= isnull([LoginName], ' ')
				,[TD align = right] = isnull([ErrorNumber], ' ')
				,[TD align = right] = isnull([ErrorSeverity], ' ')
				,[TD align = right] = isnull([ErrorState], ' ')
				,[TD]				= isnull([ErrorProcedure], '')
				,[TD align = right] = isnull([ErrorLine], ' ')
				,[TD]				= isnull([SourceTableName], ' ')
				,[TD]				= isnull([RunKey], ' ')
				,[TD]				= isnull([RunScheduleName], ' ')
			FROM #Log4Exceptions
			ORDER BY [row_number]
			FOR XML raw('tr')
				,Elements
			)

	-- Replace the entity codes and row numbers
	SET @Body = Replace(@Body, '_x0020_', space(1))
	SET @Body = Replace(@Body, '_x003D_', '=')
	SET @Body = Replace(@Body, '<tr><TRRow>0</TRRow>', '<tr bgcolor=Gainsboro>')
	SET @Body = Replace(@Body, '<tr><TRRow>1</TRRow>', '<tr bgcolor=Snow>')
	SET @Body = Replace(@Body, '<TD align = center>Failed</TD align = center>', '<td align = center bgcolor=red>Failed</TD align = center')

	IF @Body IS NULL
		SELECT @Body = '<H3><font color="Red">No ODE Errors to Report Since ' + @StartDateChar + ' </H3>'
	ELSE
		SELECT @Body = @TableHead + @Body + @TableTail

	SET @Message = @Message + @Body
	-- Add the HTML Tail:
	SET @Message = @Message + @HTMLTail
	/******************************************************************************************************/
	/******************************************************************************************************/
	/*                             Add the HTML Tail and Output the String                                */
	/******************************************************************************************************/
	/******************************************************************************************************/
	SET @Message = @Message + @HTMLTail
	SET @vault_html_string = @Message

	/******************************************************************************************************/
	/******************************************************************************************************/
	/*                            Table for List of Recent Executions:                                    */
	/******************************************************************************************************/
	/******************************************************************************************************/
	IF @vault_email_results = 1
		EXEC msdb.dbo.sp_send_dbmail @profile_name = @vault_profile_name
			,@recipients = @vault_recipients
			,@subject = @subject
			,@body = @Message
			,@body_format = 'HTML'
END