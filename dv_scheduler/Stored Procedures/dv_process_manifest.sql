CREATE procedure [dv_scheduler].[dv_process_manifest]
as
DECLARE @msg						XML
       ,@SBDialog					uniqueidentifier
	   ,@source_system_name			varchar(50)
	   ,@source_table_schema		varchar(128)		
	   ,@source_table_name			varchar(128)
	   ,@source_procedure_schema	varchar(128)
	   ,@source_procedure_name		varchar(128)
	   ,@source_table_load_type		varchar(50)
	   ,@queue						varchar(10)
	   ,@run_key					int
	   ,@delay_in_seconds			int
	   ,@delayChar					char(8)
/*********************************/
set @run_key = 1
set @delay_in_seconds = 1
/*********************************/
if not exists (select 1 from [dv_scheduler].[dv_run] where [run_key] = @run_key and [run_status] = 'Scheduled')
   raiserror('Run must be "Scheduled" to be able to Start it', 16, 1)

select @delayChar = '00' + format(CONVERT(DATETIME, DATEADD(SECOND, @delay_in_seconds, 0)), ':mm:ss');
UPDATE [dv_scheduler].[dv_run] 
	set [run_status] = 'Started'
	   ,[run_start_datetime] = SYSDATETIMEOFFSET()

while 1=1 -- The loop forcibly exits when all processing has completed
BEGIN
-- Check whether the Schedule is complete
    if not exists (
		select 1
		from [dv_scheduler].[dv_run] r
		inner join [dv_scheduler].[dv_run_manifest] m
		on m.run_key = r.run_key
		where 1=1
		  and r.run_key = @run_key
		  and isnull(m.run_status, '') <> 'Completed')
		BEGIN
		    UPDATE [dv_scheduler].[dv_run] 
			   set [run_status] = 'Completed'
	              ,[run_end_datetime] = SYSDATETIMEOFFSET()
			   where [run_key] = @run_key
			print 'Result: Success'
			BREAK
		END
-- has there been a Failure?	
	if exists (
		select 1
		from [dv_scheduler].[dv_run] r
		inner join [dv_scheduler].[dv_run_manifest] m
		on m.run_key = r.run_key
		where 1=1
		and r.run_key = @run_key
		and isnull(m.run_status, '') = 'Failed')
		BEGIN
-- If so, Is there anything to run, assuming that what is queued or running now will succeed?
			if not exists(SELECT 1 FROM [dv_scheduler].[fn_GetWaitingSchedulerTasks] (1, 'Potential'))
			BEGIN
-- If not, Fail the run.
				UPDATE [dv_scheduler].[dv_run] 
					set [run_status] = 'Failed'
	                   ,[run_end_datetime] = SYSDATETIMEOFFSET()
					where [run_key] = @run_key
				print 'Result: Failure'
				BREAK
			END
        END
-- There is still something to run so Get a list of Tasks to run:
	DECLARE manifest_cursor CURSOR FOR  
	SELECT [source_system_name]
		  ,[source_table_schema]	
		  ,[source_table_name]	
		  ,[source_procedure_schema]	
		  ,[source_procedure_name]	
		  ,[source_table_load_type]	
		  ,[queue]
	FROM [dv_scheduler].[fn_GetWaitingSchedulerTasks] (@run_key, DEFAULT)

	OPEN manifest_cursor
	FETCH NEXT FROM manifest_cursor 
	  INTO @source_system_name
		  ,@source_table_schema
		  ,@source_table_name
		  ,@source_procedure_schema 
		  ,@source_procedure_name
		  ,@source_table_load_type
		  ,@queue  
	
	WHILE @@FETCH_STATUS = 0   
	BEGIN   
	
	SET @msg = N'
	<Request>
	      <RunKey>'		  + isnull(cast(@run_key as varchar(20)), '')	+ N'</RunKey>
		  <SourceSystem>' + isnull(@source_system_name, '')				+ N'</SourceSystem>
		  <SourceSchema>' + isnull(@source_table_schema, '')			+ N'</SourceSchema>
		  <SourceTable>'  + isnull(@source_table_name, '')				+ N'</SourceTable>
		  <ProcSchema>'	  + isnull(@source_procedure_schema, '')		+ N'</ProcSchema>
		  <ProcName>'	  + isnull(@source_procedure_name, '')			+ N'</ProcName>
	</Request>'
select @queue, @msg
	BEGIN TRANSACTION
	IF @queue = '001'
	BEGIN
		BEGIN DIALOG CONVERSATION @SBDialog
			FROM SERVICE dv_scheduler_s001
			TO SERVICE  'dv_scheduler_s001'
			ON CONTRACT  dv_scheduler_c001
			WITH ENCRYPTION = OFF;
			--Send messages on Dialog
		SEND ON CONVERSATION @SBDialog
			MESSAGE TYPE dv_scheduler_m001 (@Msg)
	END
	ELSE
	BEGIN
		BEGIN DIALOG CONVERSATION @SBDialog
			FROM SERVICE dv_scheduler_s002
			TO SERVICE	'dv_scheduler_s002'
			ON CONTRACT	 dv_scheduler_c002
			WITH ENCRYPTION = OFF;
			--Send messages on Dialog
		SEND ON CONVERSATION @SBDialog
			MESSAGE TYPE dv_scheduler_m002 (@Msg)
	END
	END CONVERSATION @SBDialog
	UPDATE [dv_scheduler].[dv_run_manifest]
			SET [completed_datetime] = getdate()
               ,[run_status] = 'Queued'
               ,[row_count] = 0
               --,[session_id] = @@SPID
			WHERE [run_key] = cast(ltrim(rtrim(@run_key)) as int)
			  AND [source_system_name] = @source_system_name
			  AND [source_table_schema] = @source_table_schema
              AND [source_table_name] = @source_table_name
	COMMIT
	FETCH NEXT FROM manifest_cursor 
	  INTO @source_system_name
		  ,@source_table_schema
		  ,@source_table_name 
		  ,@source_procedure_schema 
		  ,@source_procedure_name
		  ,@source_table_load_type
		  ,@queue  
	END   
	
	CLOSE manifest_cursor   
	DEALLOCATE manifest_cursor
    WAITFOR DELAY @delayChar
END