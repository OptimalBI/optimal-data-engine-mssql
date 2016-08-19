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
,@SourceTable VARCHAR(128)         = 'Adventureworks__Sales__vIndividualCustomer'
	-- the name of the source table (this is a Stage table, which needs to exist. The script will use the meta data of the table to build the required Configuration in ODE)
	-- The name must be unique across all Data Vaults.
	-- To check, select * from ODE_Config.dbo.dv_source_table where table_name = 'YourSourceTableName' eg. 'Adventureworks__Sales__SalesOrderHeader'
,@HubName VARCHAR(128)            = 'Customer'
	-- For completely Raw Hub Sat combinations, you can leave this column as null. The Script will create a Hub using the same name as the source table.
	-- For Business hubs, specify the name of the Hub of the Ensemble, which you are adding to.
,@SourceSystem VARCHAR(128)       = 'Adventureworks'
	-- the Source System Name as it appears in select source_system_name from ODE_config.dbo.dv_source_system.
	-- Note that your Stage Table and usp need to exist in the database called: ODE_config.dv_source_system.timevault_name.
--EXECUTE [dbo].[dv_source_system_insert] 'Adventureworks', 'ode_stage', 0, 0
,@VaultName VARCHAR(128)             =  'ODE_Vault'
	--the name of the vault where the Hub and Satellite will be created.
,@HubKeyName VARCHAR(128)         = 'CustomerID'
	--the name of the unique column. The column needs to exist in your Stage Table, and should be appropriately named for the Hub, which you are building.
,@ScheduleName VARCHAR(128)      =  'TestSchedule'
	--the schedule the load is to run in. This schedule needs to exist prior to running this script.
--EXECUTE [dv_scheduler].[dv_schedule_insert] 'Test_Schedule', 'For Testing Purposes', 'Ad Hoc', 0
--
-- Defaults
DECLARE
@is_columnstore BIT = 1
	-- Note that Columnstore is only available in SQl Server Enterprise Edition.
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

if @VaultName =  @BusinessVaultName
INSERT @ExcludeColumns select @HubKeyName
select @HubName = case when isnull(@HubName, '') = '' then @SourceTable else @HubName end
--
--Working Storage
DECLARE @seqint INT
,@release_number INT
,@Description VARCHAR(256)
,@uspName VARCHAR(256)
,@abbn VARCHAR(4)
,@hub_key_column_type VARCHAR(30)
,@hub_key_column_length INT
,@hub_key_column_precision INT
,@hub_key_column_scale INT
,@hub_key_Collation_Name SYSNAME
,@hub_database SYSNAME
,@release_key INT
,@hub_key INT
,@satellite_key INT
,@hub_key_column_key INT
,@hub_source_column_key INT
,@ServerName SYSNAME
SET @uspName = 'usp_' + @SourceTable
BEGIN TRANSACTION;
BEGIN TRY
select @ServerName = @@servername
-- Uncomment this to ensure that this build only happens in the correct place.
--if @ServerName <> @DevServerName
--   begin
--   raiserror( 'This Process may only be run in the Development environment!!', 16, 1)
--   end
--
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
--
/********************************************
Hub:
********************************************/
-- Configure the Hub:
if @SatelliteOnly = 'N'
begin
SELECT @abbn = [dbo].[fn_get_next_abbreviation]()
EXECUTE @hub_key = [dbo].[dv_hub_insert] @hub_name = @HubName
,@hub_abbreviation = @abbn
,@hub_schema = 'hub'
,@hub_database = @VaultName
,@is_retired = 0
,@release_number = @release_number
end
else
begin
select @hub_key = [hub_key]
,@hub_database = [hub_database]
from [dbo].[dv_hub] where [hub_name] = @HubName
if @hub_database <> @VaultName
begin
raiserror( 'The Hub and Satellite have to exist in the same database', 16, 1)
end
end
--
/********************************************
Satellite:
********************************************/
-- Configure the Satellite:
SELECT @abbn = [dbo].[fn_get_next_abbreviation]()
EXECUTE @satellite_key = [dbo].[dv_satellite_insert] @hub_key = @hub_key
,@link_key = 0 --Dont fill in for a Hub
,@link_hub_satellite_flag = 'H'
,@satellite_name = @SourceTable
,@satellite_abbreviation = @abbn
,@satellite_schema = 'sat'
,@satellite_database = @VaultName
,@duplicate_removal_threshold = @duplicate_removal_threshold
,@is_columnstore = @is_columnstore
,@is_retired = 0
,@release_number = @release_number
--
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
--
/********************************************
Hub Key:
********************************************/
-- Create the Hub Key based on the Source Column:
SELECT @hub_key_column_type = 'varchar' --[column_type]
,@hub_key_column_length = 30 --[column_length]
,@hub_key_column_precision = 0 --[column_precision]
,@hub_key_column_scale = 0 --[column_scale]
,@hub_key_Collation_Name = null --[Collation_Name]
,@hub_source_column_key = [column_key]
FROM dv_column c
WHERE [column_key] IN (
SELECT [column_key]
FROM [dbo].[dv_satellite_column]
WHERE [satellite_key] = @satellite_key
AND [column_name] = @HubKeyName
)
--
if @SatelliteOnly = 'N'
begin
EXECUTE @hub_key_column_key = [dbo].[dv_hub_key_insert] @hub_key = @hub_key
,@hub_key_column_name = @HubKeyName
,@hub_key_column_type = @hub_key_column_type
,@hub_key_column_length = @hub_key_column_length
,@hub_key_column_precision = @hub_key_column_precision
,@hub_key_column_scale = @hub_key_column_scale
,@hub_key_Collation_Name = @hub_key_Collation_Name
,@hub_key_ordinal_position = 1
,@release_number = @release_number
end
else
begin
select @hub_key_column_key = [hub_key_column_key]
from [dbo].[dv_hub_key_column]
where [hub_key] = @hub_key
if @@rowcount > 1
begin
raiserror( 'This script does not deal with multi part Hub Keys. ', 16, 1)
end
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
--
-- hook the Hub Key up to the Source Column which will populate it:
EXECUTE [dbo].[dv_hub_column_insert] @hub_key_column_key = @hub_key_column_key
,@column_key = @hub_source_column_key
,@release_number = @release_number
--
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
SELECT case when @SatelliteOnly = 'N' then 'EXECUTE [dbo].[dv_create_hub_table] ''' + @VaultName + ''',''' + @HubName + ''',''N''' else '' end
UNION
SELECT 'EXECUTE [dbo].[dv_create_sat_table] ''' + @VaultName + ''',''' + @SourceTable + ''',''N'''
UNION
SELECT 'EXECUTE [dbo].[dv_load_source_table]
@vault_source_system_name = ''' + @SourceSystem + '''
,@vault_source_table_schema = ''' + @StageSchema + '''
,@vault_source_table_name = ''' + @SourceTable + '''
,@vault_source_load_type = ''full'''
UNION
SELECT 'select top 1000 * from ' + quotename(hub_database) + '.' + quotename(hub_schema) + '.' + quotename([dbo].[fn_get_object_name] (hub_name, 'hub'))
from [dbo].[dv_hub] where hub_name = @HubName
UNION
SELECT 'select top 1000 * from ' + quotename(satellite_database) + '.' + quotename(satellite_schema) + '.' + quotename([dbo].[fn_get_object_name] (satellite_name, 'sat'))
from [dbo].[dv_satellite] where satellite_name =  @SourceTable
--
PRINT 'succeeded';
-- Commit if successful:
COMMIT;
END TRY
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