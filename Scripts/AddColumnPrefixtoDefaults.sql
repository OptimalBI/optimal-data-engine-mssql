SET IDENTITY_INSERT [dbo].[dv_default_column] ON; 
MERGE INTO [dbo].[dv_default_column] AS trgt USING	(VALUES (1,'Hub','Object_Key',1,'h_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2),
		(4,'Lnk','Object_Key',1,'l_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2),
		(7,'Sat','Object_Key',1,'s_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2),
		(14,'Dim','Object_Key',1,'d_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2),
		(19,'Fact','Object_Key',1,'f_','%','_key','int',NULL,NULL,NULL,NULL,0,1,0,2)
			) AS src([default_column_key],[object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
	ON
		trgt.[default_column_key] = src.[default_column_key]
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
		INSERT ([default_column_key],[object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
		VALUES ([default_column_key],[object_type],[object_column_type],[ordinal_position],[column_prefix],[column_name],[column_suffix],[column_type],[column_length],[column_precision],[column_scale],[collation_Name],[is_nullable],[is_pk],[discard_flag],[release_key])
	
	;
SET IDENTITY_INSERT [dbo].[dv_default_column] OFF;



