/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/
/*  Default Release  */
SET IDENTITY_INSERT [dv_release].[dv_release_master] ON;

	MERGE INTO [dv_release].[dv_release_master] AS trgt
	USING	(VALUES
			(0,0,'<N/A>','<N/A>','<N/A>')
			) AS src([release_key],[release_number],[release_description],[reference_number],[reference_source])
	ON
		trgt.[release_key] = src.[release_key]
	WHEN MATCHED THEN
		UPDATE SET
		  [release_number] = src.[release_number]
		, [release_description] = src.[release_description]
		, [reference_number] = src.[reference_number]
		, [reference_source] = src.[reference_source]
		
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([release_key],[release_number],[release_description],[reference_number],[reference_source])
		VALUES ([release_key],[release_number],[release_description],[reference_number],[reference_source])
	WHEN NOT MATCHED BY SOURCE THEN
		DELETE
	;
SET IDENTITY_INSERT [dv_release].[dv_release_master] OFF;
/*  Default Hub  */
SET IDENTITY_INSERT [dbo].[dv_hub] ON; 
MERGE INTO [dbo].[dv_hub] AS trgt 
USING	(VALUES (0,'Default_Hub',NULL,'Default','Default',0)) AS src([hub_key],[hub_name],[hub_abbreviation],[hub_schema],[hub_database],[release_key])
	ON
		trgt.[hub_key] = src.[hub_key]
	WHEN MATCHED THEN
		UPDATE SET
			[hub_name] = src.[hub_name]
		, [hub_abbreviation] = src.[hub_abbreviation]
		, [hub_schema] = src.[hub_schema]
		, [hub_database] = src.[hub_database]
		, [release_key] = src.[release_key]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([hub_key],[hub_name],[hub_abbreviation],[hub_schema],[hub_database],[release_key])
		VALUES ([hub_key],[hub_name],[hub_abbreviation],[hub_schema],[hub_database],[release_key])
;	
SET IDENTITY_INSERT [dbo].[dv_hub] OFF;

/*  Default Link  */
SET IDENTITY_INSERT [dbo].[dv_link] ON; 
MERGE INTO [dbo].[dv_link] AS trgt USING (VALUES (0,'Default_Link',NULL,'Default','Default',0)) 
		AS src([link_key],[link_name],[link_abbreviation],[link_schema],[link_database],[release_key])
	ON
		trgt.[link_key] = src.[link_key]
	WHEN MATCHED THEN
		UPDATE SET
			[link_name] = src.[link_name]
		, [link_abbreviation] = src.[link_abbreviation]
		, [link_schema] = src.[link_schema]
		, [link_database] = src.[link_database]
		, [release_key] = src.[release_key]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([link_key],[link_name],[link_abbreviation],[link_schema],[link_database],[release_key])
		VALUES ([link_key],[link_name],[link_abbreviation],[link_schema],[link_database],[release_key])
;
SET IDENTITY_INSERT [dbo].[dv_link] OFF;


