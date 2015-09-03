SET IDENTITY_INSERT [dbo].[dv_default_column] ON; 
MERGE INTO [dbo].[dv_default_column] AS trgt 
USING	(VALUES 
        ('Hub', 'Object_Key',1,'h_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2),
		('Lnk', 'Object_Key',1,'l_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2),
		('Sat', 'Object_Key',1,'s_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2),
		('Dim', 'Object_Key',1,'d_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2),
		('Fact','Object_Key',1,'f_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2)
			) AS src([object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
	ON
		trgt.[object_type]			= src.[object_column_type] and 
		trgt.[object_column_type]	= src.[object_column_type]
	WHEN MATCHED THEN
		UPDATE SET
		  [object_type] = src.[object_type]
		, [object_column_type] = src.[object_column_type]
		, [ordinal_position] = src.[ordinal_position]
		, [column_prefix] = src.[column_prefix]
		, [column_name] = src.[column_name]
		, [column_suffix] = src.[column_suffix]
		, [column_type] = src.[column_type]
		, [column_length] = src.[column_length]
		, [column_precision] = src.[column_precision]
		, [column_scale] = src.[column_scale]
		, [collation_Name] = src.[collation_Name]
		, [is_nullable] = src.[is_nullable]
		, [is_pk] = src.[is_pk]
		, [discard_flag] = src.[discard_flag]
		, [release_key] = src.[release_key]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
		VALUES ([object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
	;
SET IDENTITY_INSERT [dbo].[dv_default_column] OFF;

SET IDENTITY_INSERT [dbo].[dv_defaults] ON; MERGE INTO [dbo].[dv_defaults] AS trgt 
USING	(VALUES 
		('Hub', 'Prefix',1,'varchar',NULL,'h_',NULL,0),
		('Lnk', 'Prefix',1,'varchar',NULL,'l_',NULL,0),
		('Sat', 'Prefix',1,'varchar',NULL,'s_',NULL,0),
		('Dim', 'Prefix',1,'varchar',NULL,'d_',NULL,0),
		('Fact','Prefix',1,'varchar',NULL,'f_',NULL,0)
		) AS src([default_type],[default_subtype],[default_sequence],[data_type],[default_integer],[default_varchar],[default_dateTime],[release_key])
	ON
		trgt.[default_type] = src.[default_type] and
		trgt.[default_subtype] = src.[default_subtype]
	WHEN MATCHED THEN
		UPDATE SET
		  [default_type] = src.[default_type]
		, [default_subtype] = src.[default_subtype]
		, [default_sequence] = src.[default_sequence]
		, [data_type] = src.[data_type]
		, [default_integer] = src.[default_integer]
		, [default_varchar] = src.[default_varchar]
		, [default_dateTime] = src.[default_dateTime]
		, [release_key] = src.[release_key]
	WHEN NOT MATCHED BY TARGET THEN
		INSERT ([default_type],[default_subtype],[default_sequence],[data_type],[default_integer],[default_varchar],[default_dateTime],[release_key])
		VALUES ([default_type],[default_subtype],[default_sequence],[data_type],[default_integer],[default_varchar],[default_dateTime],[release_key])
	;
SET IDENTITY_INSERT [dbo].[dv_defaults] OFF;
