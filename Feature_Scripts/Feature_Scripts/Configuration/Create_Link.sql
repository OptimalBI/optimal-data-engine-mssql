USE [ODE_Config]
GO
-------------------------------------------------------------------------------------------
--Populate the following Parameters:
DECLARE
--
@SatelliteOnly char(1)        = 'N'
	-- when set to "N", the script will create a Hub and Satellite combination.
	-- "Y" will cause the script to create a Satellite and hook it up to the specified Hub.
,@sprintdate CHAR(8)        = '20160803'
	-- Start Date of the current Sprint in Integer yyyymmdd (this depends on having set up a Sprint Release with the key yyymmdd00
	-- e.g. EXECUTE [dv_release].[dv_release_master_insert] 2016080100, 'Test Sprint release', 'US001', 'Jira'
,@ReleaseReference VARCHAR(50) = 'US999'
	-- User Story and/or Task numbers for the Satellite which you are building.
,@ReleaseSource VARCHAR(50)       = 'Jira'
	-- system the reference number refers to, Rally
,@SourceTable VARCHAR(128)         = 'link_Sale'
	-- the name of the source table (this is a Stage table, which needs to exist. The script will use the meta data of the table to build the required Configuration in ODE)
	-- The name must be unique across all Data Vaults.
	-- To check, select * from ODE_Config.dbo.dv_source_table where table_name = 'YourSourceTableName' eg. 'Adventureworks__Sales__SalesOrderHeader'
,@LinkName VARCHAR(128)            = 'Sale'
	-- For completely Raw Links, you can leave this column as null. The Script will create a Link using the same name as the source table.
	-- For Business Links, specify the name of the Link in the Ensemble.
,@SourceSystem VARCHAR(128)       = 'ODE'
	-- the Source System Name as it appears in select source_system_name from ODE_config.dbo.dv_source_system.
	-- Note that your Stage Table and usp need to exist in the database called: ODE_config.dv_source_system.timevault_name.
--EXECUTE [dbo].[dv_source_system_insert] 'Adventureworks', 'ode_stage', 0, 0
,@VaultName VARCHAR(128)             =  'ODE_Vault'
	--the name of the vault where the Hub and Satellite will be created.
,@ScheduleName VARCHAR(128)      =  'TestSchedule'
	--the schedule the load is to run in. This schedule needs to exist prior to running this script.
--EXECUTE [dv_scheduler].[dv_schedule_insert] 'Test_Schedule', 'For Testing Purposes', 'Ad Hoc', 0
--
DECLARE @Hub_Key_List TABLE (hub_name varchar(128), column_name varchar(128))
INSERT @Hub_Key_List  VALUES ('Customer', 'CustomerID')
INSERT @Hub_Key_List  VALUES ('Sale', 'SalesOrderNumber')
INSERT @Hub_Key_List  VALUES ('SalesPerson', 'SalesPersonID')

-- Defaults
DECLARE
@is_columnstore BIT = 1
	-- Note that Columnstore is only available in SQL Server Enterprise Edition.
,@duplicate_removal_threshold INT = 0
,@StageSchema VARCHAR(128) = 'Stage'
,@DevServerName SYSNAME = 'Ignore'
	-- You can provide a Server Name here to prevent accidentally creating keys and objects in the wrong environment.
,@BusinessVaultName VARCHAR(128) = 'Ignore'
	-- You can provide a name here to cause the Business key to be excluded from the Sat, in a specific Vault.
DECLARE @ExcludeColumns TABLE (ColumnName VARCHAR(128))
INSERT @ExcludeColumns  VALUES ('dv_stage_datetime')
	--Insert columns which should never be included in the satellites.
	-- Exclude the Hub Key from the Satellite if it is in Business Vault. Otherwise keep it.
-------------------------------------------------------------------------------------------

select @LinkName = case when isnull(@LinkName, '') = '' then @SourceTable else @LinkName end
--Working Storage
DECLARE @seqint INT
,@release_number INT
,@Description VARCHAR(256)
,@uspName VARCHAR(256)
,@abbn VARCHAR(4)
--
,@link_key INT
,@satellite_key INT
,@link_database SYSNAME
,@release_key INT
--
,@hub_name varchar(128)
,@hub_key int
,@column_name varchar(128)
,@hub_key_column_key INT
,@hub_source_column_key INT
--
,@ServerName SYSNAME
--
SET @uspName = 'usp_' + @SourceTable
BEGIN TRANSACTION;
BEGIN TRY
select @ServerName = @@servername
-- Uncomment this to ensure that this build only happens in the correct place.
--if @ServerName <> @DevServerName
--   begin
--   raiserror( 'This Process may only be run in the Development environment!!', 16, 1)
--   end
/********************************************
Release:
********************************************/
--Find the Next Release for the Sprint
SELECT TOP 1 @seqint = cast(right(cast([release_number] AS VARCHAR(100)), len(cast([release_number] AS VARCHAR(100))) - 8) AS INT)
FROM [dv_release].[dv_release_master]
WHERE left(cast([release_number] AS VARCHAR(100)), 8) = @sprintdate
ORDER BY 1 DESC
IF @@rowcount = 0
SET @release_number = cast(@sprintdate + '01' AS INT)
ELSE
SET @release_number = cast(@sprintdate + right('00' + cast(@seqint + 1 AS VARCHAR(100)), 2) AS INT)
SELECT @release_number
SET @Description = 'Load Source Table: ' + quotename(@SourceTable) + ' into ' + quotename(@VaultName)
-- Create the Release:
EXECUTE @release_key = [dv_release].[dv_release_master_insert] @release_number = @release_number -- date of the Sprint Start + ad hoc release number
,@release_description = @Description -- what the release is for
,@reference_number = @ReleaseReference
,@reference_source = @ReleaseSource
 
/********************************************
Link:
********************************************/
-- Configure the Link:
if @SatelliteOnly = 'N'
begin
SELECT @abbn = [dbo].[fn_get_next_abbreviation]()
EXECUTE @link_key = [dbo].[dv_link_insert] @link_name = @LinkName
,@link_abbreviation = @abbn
,@link_schema = 'lnk'
,@link_database = @VaultName
,@is_retired = 0
,@release_number = @release_number
end
else
begin
select @link_key = [link_key]
,@link_database = [link_database]
from [dbo].[dv_link] where [link_name] = @LinkName
if @link_database <> @VaultName
begin
raiserror( 'The Link and Satellite have to exist in the same database', 16, 1)
end
end
/********************************************
Satellite:
********************************************/
-- Configure the Satellite:
SELECT @abbn = [dbo].[fn_get_next_abbreviation]()
EXECUTE @satellite_key = [dbo].[dv_satellite_insert] @link_key = @link_key
,@hub_key = 0 --Dont fill in for a Link
,@link_hub_satellite_flag = 'L'
,@satellite_name = @SourceTable
,@satellite_abbreviation = @abbn
,@satellite_schema = 'sat'
,@satellite_database = @VaultName
,@duplicate_removal_threshold = @duplicate_removal_threshold
,@is_columnstore = @is_columnstore
,@is_retired = 0
,@release_number = @release_number
/********************************************
Source Table:
********************************************/
SELECT 'Build the Source Table with its columns: ', @StageSchema, @uspName
EXECUTE [dv_config].[dv_populate_source_table_columns] @vault_source_system = @SourceSystem
,@vault_source_schema = @StageSchema
,@vault_source_table = @SourceTable
,@vault_source_table_load_type = 'Full'
,@vault_source_procedure_schema = @StageSchema --source table is represented by the view
,@vault_source_procedure_name = @uspName
,@vault_rerun_column_insert = 0
,@vault_release_number = @release_number
--
SELECT 'Hook the Source Columns up to the Satellite:'
EXECUTE [dv_config].[dv_populate_satellite_columns] @vault_source_system = @SourceSystem
,@vault_source_schema = @StageSchema
,@vault_source_table = @SourceTable
,@vault_satellite_name = @SourceTable
,@vault_release_number = @release_number
,@vault_rerun_satellite_column_insert = 0
/********************************************
Hub Keys:
********************************************/
-- Hook up each of the Hub Keys:
while 1=1
begin
select @hub_name = hub_name, @column_name = column_name from @Hub_Key_List
if @@rowcount = 0 break
--
delete from  @Hub_Key_List where @hub_name = hub_name and @column_name = column_name
select @hub_key_column_key = hub_key_column_key
,@hub_key = h.hub_key
from [dbo].[dv_hub] h
inner join [dbo].[dv_hub_key_column] hkc on hkc.[hub_key] = h.[hub_key]
where h.hub_name = @hub_name
--
if @@rowcount > 1 raiserror( 'This script does not deal with multi part Hub Keys. ', 16, 1)
select @hub_source_column_key = c.column_key
from [dbo].[dv_column] c
inner join [dbo].[dv_source_table] st on st.source_table_key = c.table_key
inner join [dbo].[dv_source_system] ss on ss.source_system_key = st.system_key
where ss.source_system_name = @SourceSystem
and st.source_table_name = @SourceTable
and c.column_name = @column_name
--
EXECUTE [dbo].[dv_hub_link_insert]
@link_key = @link_key
,@hub_key = @hub_key
,@release_number = @release_number
EXECUTE [dbo].[dv_hub_column_insert] @hub_key_column_key = @hub_key_column_key
,@column_key = @hub_source_column_key
,@release_number = @release_number
end
--
-- Remove the Columns in the Exclude List from the Satellite:
DELETE
FROM [dbo].[dv_satellite_column]
WHERE [satellite_col_key] IN (
SELECT [satellite_col_key]
FROM dv_column c
INNER JOIN [dbo].[dv_satellite_column] sc ON sc.column_key = c.column_key
WHERE sc.[satellite_key] = @satellite_key
AND c.[column_name] IN (
SELECT *
FROM @ExcludeColumns))
/********************************************
Scheduler:
********************************************/
-- Add the Source the the required Schedule:
EXECUTE [dv_scheduler].[dv_schedule_source_table_insert] @schedule_name = @ScheduleName
,@source_system_name = @SourceSystem
,@source_table_schema = @StageSchema
,@source_table_name = @SourceTable
,@source_table_load_type = 'Full'
,@priority = 'Low'
,@queue = '001'
,@release_number = @release_number
--
/********************************************
Useful Commands:
********************************************/
--Output commands to Build the Tables and test the Load:
SELECT case when @SatelliteOnly = 'N' then 'EXECUTE [dbo].[dv_create_link_table] ''' + @VaultName + ''',''' + @LinkName + ''',''N''' else '' end
UNION
SELECT 'EXECUTE [dbo].[dv_create_sat_table] ''' + @VaultName + ''',''' + @SourceTable + ''',''N'''
UNION
SELECT 'EXECUTE [dbo].[dv_load_source_table]
@vault_source_system_name = ''' + @SourceSystem + '''
,@vault_source_table_schema = ''' + @StageSchema + '''
,@vault_source_table_name = ''' + @SourceTable + '''
,@vault_source_load_type = ''full'''
UNION
SELECT 'select top 1000 * from ' + quotename(link_database) + '.' + quotename(link_schema) + '.' + quotename([dbo].[fn_get_object_name] (link_name, 'lnk'))
from [dbo].[dv_link] where link_name = @LinkName
UNION
SELECT 'select top 1000 * from ' + quotename(satellite_database) + '.' + quotename(satellite_schema) + '.' + quotename([dbo].[fn_get_object_name] (satellite_name, 'sat'))
from [dbo].[dv_satellite] where satellite_name =  @SourceTable
--
PRINT 'succeeded';
-- Commit if successful:
COMMIT;
END TRY
/********************************************
Error Handling:
********************************************/
BEGIN CATCH
-- Return any error and Roll Back is there was a problem:
PRINT 'failed';
SELECT 'failed'
,ERROR_NUMBER() AS [errornumber]
,ERROR_SEVERITY() AS [errorseverity]
,ERROR_STATE() AS [errorstate]
,ERROR_PROCEDURE() AS [errorprocedure]
,ERROR_LINE() AS [errorline]
,ERROR_MESSAGE() AS [errormessage];
ROLLBACK;
END CATCH;