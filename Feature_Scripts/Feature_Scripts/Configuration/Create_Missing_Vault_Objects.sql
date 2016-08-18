/*
	Once you have created a set of new objects (Hubs, Links and Satellites) in Config, this script will generate the necessary statements to create the new objects.
	Missing tables are tables which are described in Configuration, but do not physically exist in the Vault.
	Output of this script is a list SQL statements. Run them to create missing tables in Data Vault.
*/
USE [ODE_Config];
GO
SET NOCOUNT ON;
 
PRINT 'Build Hub Tables';
PRINT '----------------';
DECLARE @SQL NVARCHAR(MAX)
, @SQLOUT NVARCHAR(1000)
, @ParmDefinition NVARCHAR(500);
SET @ParmDefinition = N'@SQLOutVal NVARCHAR(1000) OUTPUT';
DECLARE hub_cursor CURSOR
FOR SELECT 'select @SQLOutVal = ''EXECUTE [dbo].[dv_create_hub_table] '''''+[hub_database]+''''','''''+[hub_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([hub_database])+'.[information_schema].[tables] where table_name = ''' + [dbo].[fn_get_object_name]
([hub_name], 'Hub'
)+''')'
FROM
[dbo].[dv_hub]
WHERE [hub_key] > 0;
OPEN hub_cursor;
FETCH NEXT FROM hub_cursor INTO @SQL;
WHILE @@Fetch_Status = 0
BEGIN
SET @SQLOUT = NULL;
EXEC [sp_executesql]
@SQL
, @ParmDefinition
, @SQLOutVal = @SQLOUT OUTPUT;
IF @SQLOUT IS NOT NULL
PRINT @SQLOUT;
FETCH NEXT FROM hub_cursor INTO @SQL;
END;
CLOSE hub_cursor;
DEALLOCATE hub_cursor;
PRINT '';
PRINT 'Build Link Tables';
PRINT '----------------';
DECLARE link_cursor CURSOR
FOR SELECT 'select @SQLOutVal = ''EXECUTE [dbo].[dv_create_link_table] '''''+[link_database]+''''','''''+[link_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([link_database])+'.[information_schema].[tables] where table_name = ''' + [dbo].[fn_get_object_name]
([link_name], 'Lnk'
)+''')'
FROM
[dbo].[dv_link]
WHERE [link_key] > 0;
OPEN link_cursor;
FETCH NEXT FROM link_cursor INTO @SQL;
WHILE @@Fetch_Status = 0
BEGIN
SET @SQLOUT = NULL;
EXEC [sp_executesql]
@SQL
, @ParmDefinition
, @SQLOutVal = @SQLOUT OUTPUT;
IF @SQLOUT IS NOT NULL
PRINT @SQLOUT;
FETCH NEXT FROM link_cursor INTO @SQL;
END;
CLOSE link_cursor;
DEALLOCATE link_cursor;
PRINT '';
PRINT 'Build Sat Tables';
PRINT '----------------';
DECLARE sat_cursor CURSOR
FOR SELECT 'select @SQLOutVal = ''EXECUTE [dbo].[dv_create_sat_table] '''''+[satellite_database]+''''','''''+[satellite_name]+''''',''''N''''''
where not exists (select 1 from '+QUOTENAME([satellite_database])+'.[information_schema].[tables] where table_name = ''' + [dbo].[fn_get_object_name]
([satellite_name], 'Sat'
)+''')'
FROM
[dbo].[dv_satellite]
WHERE [satellite_key] > 0;
OPEN sat_cursor;
FETCH NEXT FROM sat_cursor INTO @SQL;
WHILE @@Fetch_Status = 0
BEGIN
SET @SQLOUT = NULL;
EXEC [sp_executesql]
@SQL
, @ParmDefinition
, @SQLOutVal = @SQLOUT OUTPUT;
IF @SQLOUT IS NOT NULL
PRINT @SQLOUT;
FETCH NEXT FROM sat_cursor INTO @SQL;
END;
CLOSE sat_cursor;
DEALLOCATE sat_cursor;