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
