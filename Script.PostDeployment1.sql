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
GO

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
GO

SET IDENTITY_INSERT [dv_release].[dv_release_master] OFF;
GO
/*  Default Ref Function  */
--SET IDENTITY_INSERT [dbo].[dv_ref_function] ON;
--GO

--MERGE INTO [dbo].[dv_ref_function] AS trgt 
--USING	(VALUES (0,'Default_Ref_Function', 'Default','Default',0)) AS src([ref_function_key],[ref_function_name],[ref_function],[ref_function_arguments],[release_key])
--	ON
--		trgt.[ref_function_key] = src.[ref_function_key]
--	WHEN MATCHED THEN
--		UPDATE SET
--		  [ref_function_name]		= src.[ref_function_name]
--		, [ref_function]			= src.[ref_function]
--		, [ref_function_arguments]	= src.[ref_function_arguments]
--		, [release_key]			    = src.[release_key]
--	WHEN NOT MATCHED BY TARGET THEN
--		INSERT ([ref_function_key],[ref_function_name],[ref_function],[ref_function_arguments],[release_key])
--		VALUES ([ref_function_key],[ref_function_name],[ref_function],[ref_function_arguments],[release_key])
--;
--GO

--SET IDENTITY_INSERT [dbo].[dv_ref_function] OFF;
--GO


/*  Default SourceSystem  */
--SET IDENTITY_INSERT [dbo].[dv_source_system] ON;
--GO

--MERGE INTO [dbo].[dv_source_system] AS trgt 
--USING	(VALUES (0,'Default_Source_System',0)) AS src([source_system_key],[source_system_name],[release_key])
--	ON
--		trgt.[source_system_key] = src.[source_system_key]
--	WHEN MATCHED THEN
--		UPDATE SET
--		  [source_system_name] = src.[source_system_name]
--		, [release_key] = src.[release_key]
--	WHEN NOT MATCHED BY TARGET THEN
--		INSERT ([source_system_key],[source_system_name],[release_key])
--		VALUES ([source_system_key],[source_system_name],[release_key])
--;
--GO

--SET IDENTITY_INSERT [dbo].[dv_source_system] OFF;
--GO

--/*  Default SourceTable  */
--SET IDENTITY_INSERT [dbo].[dv_source_table] ON;
--GO

--MERGE INTO [dbo].[dv_source_table] AS trgt 
--USING	(VALUES (0,0,'Default_Source_Table_Schema','Default_Source_Table_Name', 'Full',0)) AS src([source_table_key],[system_key],[source_table_schema],[source_table_name],[source_table_load_type],[release_key])
--	ON
--		trgt.[source_table_key] = src.[source_table_key]
--	WHEN MATCHED THEN
--		UPDATE SET
--		 [system_key]			   = src.[system_key]
--		,[source_table_schema]	   = src.[source_table_schema]
--		,[source_table_name]	   = src.[source_table_name]
--		,[source_table_load_type]  = src.[source_table_load_type]
--		,[release_key]			   = src.[release_key]
--	WHEN NOT MATCHED BY TARGET THEN
--		INSERT ([source_table_key],[system_key],[source_table_schema],[source_table_name],[source_table_load_type],[release_key])
--		VALUES ([source_table_key],[system_key],[source_table_schema],[source_table_name],[source_table_load_type],[release_key])
--;
--GO

--SET IDENTITY_INSERT [dbo].[dv_source_table] OFF;
--GO

/*  Default Column  */
--SET IDENTITY_INSERT [dbo].[dv_column] ON;
--GO

--MERGE INTO [dbo].[dv_column] AS trgt 
--USING	(VALUES (0,0,'Default_Column_Name', 'Default',0,0)) AS src([column_key],[table_key],[column_name],[column_type],[source_ordinal_position],[release_key])
--	ON
--		trgt.[column_key] = src.[column_key]
--	WHEN MATCHED THEN
--		UPDATE SET
--		 [table_key]					= src.[table_key]
--		,[column_name]					= src.[column_name]
--		,[column_type]					= src.[column_type]
--		,[source_ordinal_position]		= src.[source_ordinal_position]
--		,[release_key]					= src.[release_key]
--	WHEN NOT MATCHED BY TARGET THEN
--		INSERT ([column_key],[table_key],[column_name],[column_type],[source_ordinal_position],[release_key])
--		VALUES ([column_key],[table_key],[column_name],[column_type],[source_ordinal_position],[release_key])
--;
--GO

--SET IDENTITY_INSERT [dbo].[dv_column] OFF;
--GO
/*  Default Hub  */
SET IDENTITY_INSERT [dbo].[dv_hub] ON;
GO

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
GO

SET IDENTITY_INSERT [dbo].[dv_hub] OFF;
GO

/*  Default Link  */
SET IDENTITY_INSERT [dbo].[dv_link] ON;
GO

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
GO

SET IDENTITY_INSERT [dbo].[dv_link] OFF;
GO


MERGE INTO [dbo].[dv_defaults] AS trgt 
USING (VALUES	('Global','LowDate',1,'datetime',NULL,NULL,'Jan  1 1900 12:00:00:000AM',0),
				('Global','HighDate',1,'datetime',NULL,NULL,'Dec 31 9999 12:00:00:000AM',0),
				('Global','DefaultLoadDateTime',1,'varchar',NULL,'sysdatetimeoffset()',NULL,0),
				('Global','FailedLookupKey',1,'int',-999,NULL,NULL,0),
				('Global','UnknownValue',1,'varchar',NULL,'<Unknown>',NULL,0),
				('Global','NAValue',1,'varchar',NULL,'<N/A>',NULL,0),
				('Global','MissingValue',1,'varchar',NULL,'<Missing>',NULL,0),
				('Global','Not RequiredValue',1,'varchar',NULL,'<Not Required>',NULL,0),
				('Hub','Prefix',1,'varchar',NULL,'h_',NULL,0),
				('Hub','Schema',1,'varchar',NULL,'Hub',NULL,0),
				('Hub','Filegroup',1,'varchar',NULL,'PRIMARY',NULL,0),
				('HubSurrogate','Suffix',1,'varchar',NULL,'_key',NULL,0),
				('Lnk','Prefix',1,'varchar',NULL,'l_',NULL,0),
				('Lnk','Schema',1,'varchar',NULL,'Lnk',NULL,0),
				('Lnk','Filegroup',1,'varchar',NULL,'PRIMARY',NULL,0),
				('LnkSurrogate','Suffix',1,'varchar',NULL,'_key',NULL,0),
				('Sat','Prefix',1,'varchar',NULL,'s_',NULL,0),
				('Sat','Schema',1,'varchar',NULL,'Sat',NULL,0),
				('Sat','Filegroup',1,'varchar',NULL,'PRIMARY',NULL,0),
				('SatSurrogate','Suffix',1,'varchar',NULL,'_key',NULL,0),
				('dv_col_metrics','RunType',1,'varchar',NULL,'Weekly', NULL, 0),
				('Scheduler','PollDelayInSeconds',1,'int',30,NULL,NULL,0)
			) AS src([default_type],[default_subtype],[default_sequence],[data_type],[default_integer],[default_varchar],[default_dateTime],[release_key])
	ON
		trgt.[default_type]     = src.[default_type] and 
		trgt.[default_subtype]  = src.[default_subtype]
	WHEN MATCHED THEN
		UPDATE SET
		  [default_type]		= src.[default_type]
		, [default_subtype]		= src.[default_subtype]
		, [default_sequence]	= src.[default_sequence]
		, [data_type]			= src.[data_type]
		, [default_integer]		= src.[default_integer]
		, [default_varchar]		= src.[default_varchar]
		, [default_dateTime]	= src.[default_dateTime]
		, [release_key]			= src.[release_key]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([default_type],[default_subtype],[default_sequence],[data_type],[default_integer],[default_varchar],[default_dateTime],[release_key])
		VALUES ([default_type],[default_subtype],[default_sequence],[data_type],[default_integer],[default_varchar],[default_dateTime],[release_key])
	;
GO

MERGE INTO [dbo].[dv_default_column] AS trgt 
USING	(VALUES ('Hub','Object_Key',1,'h_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,0),
			    ('Hub','Load_Date_Time',2,NULL,'dv_load_date_time',NULL,'datetimeoffset',NULL,7,NULL,NULL,0,0,0,0),
			    ('Hub','Data_Source',3,NULL,'dv_record_source',NULL,'varchar',50,NULL,NULL,NULL,0,0,0,0),
			    ('Lnk','Object_Key',1,'l_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,0),
			    ('Lnk','Load_Date_Time',2,NULL,'dv_load_date_time',NULL,'datetimeoffset',NULL,7,NULL,NULL,0,0,0,0),
			    ('Lnk','Data_Source',3,NULL,'dv_record_source',NULL,'varchar',50,NULL,NULL,NULL,0,0,0,0),
			    ('Sat','Object_Key',1,'s_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,0),
			    ('Sat','Source_Date_Time',3,NULL,'dv_source_date_time',NULL,'datetimeoffset',NULL,7,NULL,NULL,0,0,0,0),
			    ('Sat','Data_Source',4,NULL,'dv_record_source',NULL,'varchar',50,NULL,NULL,NULL,0,0,0,0),
			    ('Sat','Current_Row',5,NULL,'dv_row_is_current',NULL,'bit',NULL,NULL,NULL,NULL,0,0,0,0),
			    ('Sat','Tombstone_Indicator',6,NULL,'dv_is_tombstone',NULL,'bit',NULL,NULL,NULL,NULL,0,0,0,0),
			    ('Sat','Version_Start_Date',7,NULL,'dv_rowstartdate',NULL,'datetimeoffset',NULL,7,NULL,NULL,0,0,0,0),
			    ('Sat','Version_End_Date',8,NULL,'dv_rowenddate',NULL,'datetimeoffset',NULL,7,NULL,NULL,0,0,0,0)			  
			) AS src([object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
	ON
		trgt.[object_type]			= src.[object_type] and 
		trgt.[object_column_type]	= src.[object_column_type]
	WHEN MATCHED THEN
		UPDATE SET
		  [object_type]				= src.[object_type]
		, [object_column_type]		= src.[object_column_type]
		, [ordinal_position]		= src.[ordinal_position]
		, [column_prefix]			= src.[column_prefix]
		, [column_name]				= src.[column_name]
		, [column_suffix]			= src.[column_suffix]
		, [column_type]				= src.[column_type]
		, [column_length]			= src.[column_length]
		, [column_precision]		= src.[column_precision]
		, [column_scale]			= src.[column_scale]
		, [collation_Name]			= src.[collation_Name]
		, [is_nullable]				= src.[is_nullable]
		, [is_pk]					= src.[is_pk]
		, [discard_flag]			= src.[discard_flag]
		, [release_key]				= src.[release_key]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
		VALUES ([object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
	;
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 1
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        1
      , 'Showstopper/Critical Failure'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Showstopper/Critical Failure'
    WHERE
        [SeverityId] = 1
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 2
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        2
      , 'Severe Failure'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Severe Failure'
    WHERE
        [SeverityId] = 2
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 4
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        4
      , 'Major Failure'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Major Failure'
    WHERE
        [SeverityId] = 4
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 8
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        8
      , 'Moderate Failure'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Moderate Failure'
    WHERE
        [SeverityId] = 8
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 16
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        16
      , 'Minor Failure'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Minor Failure'
    WHERE
        [SeverityId] = 16
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 32
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        32
      , 'Concurrency Violation'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Concurrency Violation'
    WHERE
        [SeverityId] = 32
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 64
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        64
      , 'Reserved for future Use 1'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Reserved for future Use 1'
    WHERE
        [SeverityId] = 64
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 128
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        128
      , 'Reserved for future Use 2'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Reserved for future Use 2'
    WHERE
        [SeverityId] = 128
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 256
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        256
      , 'Informational'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Informational'
    WHERE
        [SeverityId] = 256
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 512
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        512
      , 'Success'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Success'
    WHERE
        [SeverityId] = 512
GO

IF NOT EXISTS
    (
        SELECT 1 FROM [log4].[Severity] WHERE [SeverityId] = 1024
    )
  INSERT INTO [log4].[Severity]
      (
        [SeverityId]
      , [SeverityName]
      )
  VALUES
      (
        1024
      , 'Debug'
      )
ELSE
    UPDATE
        [log4].[Severity]
    SET
          [SeverityName] = 'Debug'
    WHERE
        [SeverityId] = 1024
GO

MERGE INTO [log4].[JournalControl] AS trgt 
USING	(VALUES ('dv_create_DV_table'				,'OFF'),
				('dv_create_hub_table'				,'OFF'),
				('dv_create_link_table'				,'OFF'),
				('dv_create_sat_table'				,'OFF'),
				('dv_load_hub_table'				,'OFF'),
				('dv_load_sat_table'				,'OFF'),
				('dv_load_source_table_key_lookup'	,'OFF'),
				('dv_load_link_table'				,'OFF'),
				('dv_load_sats_for_source_table'	,'OFF'),
				('dv_load_source_table'				,'OFF'),
				('IntegrityChecks'					,'ON'),
				('SYSTEM_OVERRIDE'					,'ON'),
				('SYSTEM_DEFAULT'					,'OFF')
			) AS src([ModuleName],[OnOffSwitch])
	ON
		trgt.[ModuleName]			= src.[ModuleName] 
	WHEN MATCHED THEN
		UPDATE SET
		  [OnOffSwitch]				= src.[OnOffSwitch]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([ModuleName],[OnOffSwitch])
		VALUES ([ModuleName],[OnOffSwitch])
	;
select * from [log4].[JournalControl]
GO
