/*
	When business rules change, it is desirable to be able to audit the different versions of the rule over time.
	One method of doing this is the “Version” the Source Table to represent the new Rule(s).
*/
USE [ODE_Config];
GO
/******************************
Set the Parameters here:
*****************************/
-- Provide the name of the Source as it is in [dbo].[dv_source_table]
DECLARE @OldSourceTableName VARCHAR(128) = 'Sales__Customer'
	-- Provide the New name for your Source - usually suffixed with "_Vnnn" to differentiate a new version
, @NewSourceTableName VARCHAR(128) = 'Sales__Customer_V001'
	-- If there is a Stored Procedure, provide the new name - usually suffixed with the same "_Vnnn" as the Source.
	-- If no stored Procedure is required, set to a Null Value ('').
, @NewSourceProcedureSchema VARCHAR(128) = 'Stage'
, @NewSourceProcedureName VARCHAR(128) = 'usp_Sales__Customer_V001'
	-- If you are building an Incremental Release, provide your Release Number
 , @ReleaseNumber INT = 0;
/*****************************/

DECLARE @Release_Key INT;
-- Get the reease Key for use below:
SELECT @Release_Key = [release_key] FROM
 [dv_release].[dv_release_master]
WHERE [release_number] = @ReleaseNumber;
BEGIN TRANSACTION;
BEGIN TRY
 -- Create the New Source:
 DECLARE @System_key INT
 , @Source_table_schema VARCHAR(128)
 , @Source_table_name VARCHAR(128)
 , @Source_table_load_type VARCHAR(50)
 , @Source_procedure_schema VARCHAR(128)
 , @Source_procedure_name VARCHAR(128)
 , @Release_number INT
 , @New_source_table_key INT;
 SELECT @Release_Key
 , @ReleaseNumber;
 SELECT @System_key = [system_key]
 , @Source_table_schema = [source_table_schema]
 , @Source_table_name = @NewSourceTableName
 , @Source_table_load_type = [source_table_load_type]
 , @Source_procedure_schema = @NewSourceProcedureSchema
 , @Source_procedure_name = @NewSourceProcedureName
 , @Release_number = @ReleaseNumber
 FROM [dbo].[dv_source_table]
 WHERE [source_table_name] = @OldSourceTableName;
 SELECT @System_key
 , @Source_table_schema
 , @Release_number;
 EXECUTE @New_source_table_key = [dbo].[dv_source_table_insert]
 @System_key = @System_key
 , @Source_table_schema = @Source_table_schema
 , @Source_table_name = @Source_table_name
 , @Source_table_load_type = @Source_table_load_type
 , @Source_procedure_schema = @Source_procedure_schema
 , @Source_procedure_name = @Source_procedure_name
 , @is_retired = 0
 , @Release_number = @Release_number;
 -- Hook All Columns, which were related to the Old Source, to the New Source:
 UPDATE [dbo].[dv_column]
 SET
 [table_key] = @New_source_table_key
 , [release_key] = @Release_Key FROM [dbo].[dv_source_system] [ss]
 INNER JOIN [dbo].[dv_source_table] [st]
 ON [ss].[source_system_key] = [st].[system_key]
 INNER JOIN [dbo].[dv_column]
 ON [dv_column].[table_key] = [st].[source_table_key]
 WHERE [st].[source_table_name] = @OldSourceTableName;
 -- Hook the new Source into the place in the Loading Hierarchy, where The Old Source used to be:
 UPDATE [sth]
 SET
 [source_table_key] = @New_source_table_key
 , [release_key] = @Release_Key FROM [dbo].[dv_source_system] [ss]
 INNER JOIN [dbo].[dv_source_table] [st]
 ON [ss].[source_system_key] = [st].[system_key]
 INNER JOIN [dv_scheduler].[dv_source_table_hierarchy] [sth]
 ON [sth].[source_table_key] = [st].[source_table_key]
 WHERE [st].[source_table_name] = @OldSourceTableName;
 -- Hook the New source into the Schedule(s) to which the Old source belonged:
 UPDATE [sth]
 SET
 [prior_table_key] = @New_source_table_key
 , [release_key] = @Release_Key FROM [dbo].[dv_source_system] [ss]
 INNER JOIN [dbo].[dv_source_table] [st]
 ON [ss].[source_system_key] = [st].[system_key]
 INNER JOIN [dv_scheduler].[dv_source_table_hierarchy] [sth]
 ON [sth].[prior_table_key] = [st].[source_table_key]
 WHERE [st].[source_table_name] = @OldSourceTableName;
 UPDATE [sst]
 SET
 [source_table_key] = @New_source_table_key
 , [release_key] = @Release_Key FROM [dbo].[dv_source_system] [ss]
 INNER JOIN [dbo].[dv_source_table] [st]
 ON [ss].[source_system_key] = [st].[system_key]
 INNER JOIN [dv_scheduler].[dv_schedule_source_table] [sst]
 ON [sst].[source_table_key] = [st].[source_table_key]
 WHERE [st].[source_table_name] = @OldSourceTableName;
UPDATE [dbo].[dv_source_table]
SET [is_retired] = 1 
 WHERE [source_table_name] = @OldSourceTableName;
 PRINT 'succeeded';
 -- Commit if successful:
 COMMIT;
END TRY
BEGIN CATCH
 -- Return any error and Roll Back is there was a problem:
 PRINT 'failed';
 SELECT 'failed'
 , ERROR_NUMBER() AS [errornumber]
 , ERROR_SEVERITY() AS [errorseverity]
 , ERROR_STATE() AS [errorstate]
 , ERROR_PROCEDURE() AS [errorprocedure]
 , ERROR_LINE() AS [errorline]
 , ERROR_MESSAGE() AS [errormessage];
 ROLLBACK;
END CATCH;