/*
	This script prepares statements for deleting all the Configuration records for the particular release number.
	Run these statements to back out the release.
	If record existed before the release, but was changed under the release number, this script won't allow to roll back to previous record state.
*/
USE [ODE_Config]
GO
----------------------------------------------------------
--Set the release number to be backed out
DECLARE @release_number INT = 20160101
----------------------------------------------------------

SET NOCOUNT ON;

DECLARE @temptab TABLE (column_to_del INT)
DECLARE @AllTables TABLE (
 dv_schema_name VARCHAR(128)
 ,dv_table_name VARCHAR(128)
 ,dv_key_name VARCHAR(128)
 ,dv_load_order INT)

DECLARE @release_key INT 
SELECT @release_key = [release_key] FROM [dv_release].[dv_release_master] WHERE [release_number] = @release_number


INSERT @AllTables SELECT * FROM [dv_release].[fn_config_table_list]()

DECLARE @schema VARCHAR(128)
,@table VARCHAR(128)
,@key VARCHAR(128)
,@order INT
,@SQL VARCHAR(max)
,@SQLOUT VARCHAR(1000)
,@ParmDefinition VARCHAR(500)
,@output VARCHAR(1000);

SET @ParmDefinition = N'@SQLOutVal VARCHAR(1000) OUTPUT';

PRINT '--Deletion statements. Execute in this order'

DECLARE curTables CURSOR
FOR SELECT *
FROM @AllTables
ORDER BY dv_load_order DESC

OPEN curTables

FETCH NEXT
FROM curTables
INTO @schema, @table, @key, @order

WHILE @@FETCH_STATUS = 0
BEGIN
 SET @SQL = 'SELECT ' + @key + ' FROM ' + @schema + '.' + @table + ' WHERE release_key = ' + CAST(@release_key AS VARCHAR(10))

 INSERT INTO @temptab
 EXEC sp_executesql @sql

 IF (SELECT count(*) FROM @temptab) > 0
 BEGIN
 PRINT ''
 DECLARE subcur CURSOR
 FOR
 SELECT 'EXECUTE ' + @schema + '.' + @table + '_delete ' + CAST(column_to_del AS VARCHAR(10))
 FROM @temptab

 OPEN subcur

 FETCH NEXT
 FROM subcur
 INTO @output

 WHILE @@FETCH_STATUS = 0
 BEGIN
 PRINT REPLACE(@output, 'dv_hub_key_column_delete', 'dv_hub_key_delete')

 FETCH NEXT
 FROM subcur
 INTO @output
 END

 CLOSE subcur
 DEALLOCATE subcur
 PRINT '--------------------------'
 END

 DELETE
 FROM @temptab

 FETCH NEXT
 FROM curTables
 INTO @schema, @table, @key, @order
END

CLOSE curTables
DEALLOCATE curTables