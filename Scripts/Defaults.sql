MERGE INTO [dbo].[dv_defaults] AS trgt 
USING (VALUES	('Global','LowDate',1,'datetime',NULL,NULL,'Jan  1 1900 12:00:00:000AM',0),
				('Global','HighDate',1,'datetime',NULL,NULL,'Dec 31 9999 12:00:00:000AM',0),
				('Global','DefaultLoadDateTime',1,'varchar',NULL,'sysdatetimeoffset()',NULL,0),
				('Global','FailedLookupKey',1,'int',-999,NULL,NULL,0),
				('Global','UnknownValue',1,'varchar',NULL,'',NULL,0),
				('Global','NAValue',1,'varchar',NULL,'',NULL,0),
				('Global','MissingValue',1,'varchar',NULL,'',NULL,0),
				('Global','Not RequiredValue',1,'varchar',NULL,'',NULL,0),
				('Hub','Prefix',1,'varchar',NULL,'',NULL,0),
				('Hub','Schema',1,'varchar',NULL,'Hub',NULL,0),
				('Hub','Filegroup',1,'varchar',NULL,'PRIMARY',NULL,0),
				('HubSurrogate','Suffix',1,'varchar',NULL,'_key',NULL,0),
				('Lnk','Prefix',1,'varchar',NULL,'',NULL,0),
				('Lnk','Schema',1,'varchar',NULL,'Lnk',NULL,0),
				('Lnk','Filegroup',1,'varchar',NULL,'PRIMARY',NULL,0),
				('LnkSurrogate','Suffix',1,'varchar',NULL,'_key',NULL,0),
				('Sat','Prefix',1,'varchar',NULL,'',NULL,0),
				('Sat','Schema',1,'varchar',NULL,'Sat',NULL,0),
				('Sat','Filegroup',1,'varchar',NULL,'PRIMARY',NULL,0),
				('SatSurrogate','Suffix',1,'varchar',NULL,'_key',NULL,0),
				('Dim','Prefix',1,'varchar',NULL,'',NULL,0),
				('Dim','Schema',1,'varchar',NULL,'Dim',NULL,0),
				('Dim','Filegroup',1,'varchar',NULL,'PRIMARY',NULL,0),
				('DimSurrogate','Suffix',1,'varchar',NULL,'_key',NULL,0),
				('Fact','Prefix',1,'varchar',NULL,'',NULL,0),
				('Fact','Schema',1,'varchar',NULL,'Fact',NULL,0),
				('Fact','Filegroup',1,'varchar',NULL,'PRIMARY',NULL,0),
				('FactSurrogate','Suffix',1,'varchar',NULL,'_key',NULL,0),
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
			    ('Sat','Version_End_Date',8,NULL,'dv_rowenddate',NULL,'datetimeoffset',NULL,7,NULL,NULL,0,0,0,0),
			    ('Dim','Object_Key',1,'d_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,0),
			    ('Dim','Current_Row',2,NULL,'dv_row_is_current',NULL,'bit',NULL,NULL,NULL,NULL,0,0,0,0),
			    ('Dim','Tombstone_Indicator',3,NULL,'dim_is_tombstone',NULL,'bit',NULL,NULL,NULL,NULL,0,0,0,0),
			    ('Dim','Version_Start_Date',4,NULL,'dim_rowstartdate',NULL,'datetimeoffset',NULL,7,NULL,NULL,0,0,0,0),
			    ('Dim','Version_End_Date',5,NULL,'dim_rowenddate',NULL,'datetimeoffset',NULL,7,NULL,NULL,0,0,0,0),
			    ('Fact','Object_Key',1,'f_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,0)
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

