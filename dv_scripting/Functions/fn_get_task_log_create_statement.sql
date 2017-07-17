
CREATE 
FUNCTION [dv_scripting].[fn_get_task_log_create_statement] 
(@satellite_database VARCHAR(128))
RETURNS varchar(1024)
AS
BEGIN
DECLARE @SQL VARCHAR(MAX) = ''
       ,@crlf char(2) = CHAR(13) + CHAR(10)
SET @SQL += 'IF NOT EXISTS (select 1 from ' + QUOTENAME(@satellite_database) + '.INFORMATION_SCHEMA.TABLES where TABLE_TYPE = ''BASE TABLE'' and TABLE_SCHEMA = ''dbo'' and TABLE_NAME = ''dv_task_state'')' + @crlf 
SET @SQL += 'CREATE TABLE ' + QUOTENAME(@satellite_database) + '.[dbo].[dv_task_state](
	[task_state_key] INT IDENTITY(1,1) NOT NULL,
	[source_table_key] INT,
	[source_unique_name] VARCHAR(128),
	[object_key] INT,
	[object_type] VARCHAR(50),
	[object_name] VARCHAR(128),
	[procedure_name] VARCHAR(128),
	[high_water_date] DATETIMEOFFSET(7),
	[source_high_water_lsn] BINARY(10),
	[source_high_water_date] VARCHAR(50),
	[task_start_datetime] DATETIMEOFFSET(7),
	[task_end_datetime] DATETIMEOFFSET(7),
	[rows_inserted] BIGINT,
	[rows_updated] BIGINT,
	[rows_deleted] BIGINT,
	[session_id] BIGINT,
	[run_key] BIGINT,
	[updated_by] VARCHAR(128) DEFAULT SUSER_NAME(),
	[update_date_time] DATETIMEOFFSET(7) DEFAULT SYSDATETIMEOFFSET(),
PRIMARY KEY CLUSTERED ([task_state_key] ASC))
'
RETURN @SQL

END