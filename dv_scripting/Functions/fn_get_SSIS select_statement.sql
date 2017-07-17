
CREATE
FUNCTION [dv_scripting].[fn_get_SSIS select_statement]  
(@source_unique_name	VARCHAR(128)
,@function_type VARCHAR(10))
/********************************************************************************************
This takes a source_table_name and returns the name of the function, which ODE provides for data access.
Function Types are:
    "Full" - get all data at a point in time;
	"Delta" - get net changes between 2 dates;
SELECT [dv_scripting].[fn_get_SSIS select_statement]('DLR__DL__DLAPP','Delta')
SELECT [dv_scripting].[fn_get_SSIS select_statement]('DLR__DL__DLAPP','Full')
********************************************************************************************/
RETURNS varchar(4000)
AS
BEGIN
DECLARE @FullSrcTechColList		VARCHAR(4000)	= ''
       ,@DeltaSrcTechColList	VARCHAR(4000)	= ''
	   ,@SourcePayload			VARCHAR(4000)	= ''
	   ,@FullSrcFunc			VARCHAR(300)	= ''
       ,@crlf					CHAR(2)			= CHAR(13) + CHAR(10)
	   ,@func_prefix			VARCHAR(128)
	   ,@func_suffix_full		VARCHAR(128)
	   ,@func_suffix_delta		VARCHAR(128)
	   ,@func_schema_full		VARCHAR(128)
	   ,@func_schema_delta		VARCHAR(128) 
	   ,@func_name_full			VARCHAR(256)
	   ,@func_name_delta		VARCHAR(256)
	   ,@func_call_full			VARCHAR(4000)
	   ,@func_call_delta		VARCHAR(4000) 
	   ,@load_type				VARCHAR(50)
	   ,@src_cdc_action			VARCHAR(128) 
	   ,@SQL					VARCHAR(4000) 

SELECT @FullSrcTechColList =  
		CASE WHEN s.load_type = 'MSSQLcdc' then quotename(f.column_name) + ' = CONVERT(VARCHAR(2),NULL)'  + ', ' + quotename(g.column_name) + ' = CONVERT(VARCHAR(50),NULL,1), ' + quotename(h.column_name) + ' = CONVERT(VARCHAR(50),NULL,1)'
		     WHEN s.load_type = 'ODEcdc' then quotename(a.column_name) + ',' + quotename(b.column_name)
			 ELSE '' END
,@DeltaSrcTechColList =  
		CASE WHEN s.load_type = 'MSSQLcdc' then quotename(f.column_name) + ' = CAST(CASE WHEN ' + quotename(c.column_name) + ' = 1 THEN ''D'' ELSE ''U'' END  AS VARCHAR(2)), ' + quotename(g.column_name) + ' = CONVERT(VARCHAR(50),' +  quotename(d.column_name) + ',1), ' + quotename(h.column_name) + ' = CONVERT(VARCHAR(50),' + quotename(e.column_name) + ',1)'
		     WHEN s.load_type = 'ODEcdc' then quotename(a.column_name) + ',' + quotename(b.column_name)
			 ELSE '' END
,@func_prefix = CAST([dbo].[fn_get_default_value] ('Prefix'
		, CASE WHEN s.load_type = 'MSSQLcdc' THEN 'MSSQL_AccessFunction' 
		       WHEN s.load_type = 'ODEcdc'   THEN 'ODE_AccessFunction' 
			   END
		) as VARCHAR(128))
,@func_suffix_full = CAST([dbo].[fn_get_default_value] ('Suffix_' +
          CASE WHEN s.load_type = 'MSSQLcdc' THEN 'all' 
		       WHEN s.load_type = 'ODEcdc'   THEN 'pit' 
			   END 
		, CASE WHEN s.load_type = 'MSSQLcdc' THEN 'MSSQL_AccessFunction' 
		       WHEN s.load_type = 'ODEcdc'   THEN 'ODE_AccessFunction' 
			   ELSE '' END
		) as VARCHAR(128))
,@func_suffix_delta = CAST([dbo].[fn_get_default_value] ('Suffix_' +
          CASE WHEN s.load_type = 'MSSQLcdc' THEN 'all' 
		       WHEN s.load_type = 'ODEcdc'   THEN 'all' 
			   END 
		, CASE WHEN s.load_type = 'MSSQLcdc' THEN 'MSSQL_AccessFunction' 
		       WHEN s.load_type = 'ODEcdc'   THEN 'ODE_AccessFunction' 
			   END
		) as VARCHAR(128))
,@func_schema_full = CASE WHEN s.load_type = 'MSSQLcdc' THEN s.source_table_schma
						  WHEN s.load_type = 'ODEcdc' THEN CAST([dbo].[fn_get_default_value] ('Schema', 'ODE_AccessFunction') as VARCHAR(128))
						  ELSE s.source_table_schma
						  END
,@func_schema_delta = CASE WHEN s.load_type = 'MSSQLcdc'
                           THEN CAST([dbo].[fn_get_default_value] ('Schema', 'MSSQL_AccessFunction') as VARCHAR(128))
						   WHEN s.load_type = 'ODEcdc' THEN CAST([dbo].[fn_get_default_value] ('Schema', 'ODE_AccessFunction') as VARCHAR(128))
						   ELSE s.source_table_schma
						   END
,@func_name_full	= s.source_table_nme

,@func_name_delta	= s.source_table_nme
,@load_type			= s.Load_type
,@src_cdc_action    = c.column_name

FROM [dbo].[dv_source_table] s 
OUTER APPLY (SELECT column_name FROM [dbo].[dv_default_column] WHERE [object_type] = 'CdcStgODE' AND [object_column_type]   = 'CDC_Action') a
OUTER APPLY (SELECT column_name FROM [dbo].[dv_default_column] WHERE [object_type] = 'CdcStgODE' AND [object_column_type]   = 'CDC_StartDate') b
OUTER APPLY (SELECT column_name FROM [dbo].[dv_default_column] WHERE [object_type] = 'CdcSrcMSSQL' AND [object_column_type] = 'CDC_Action') c
OUTER APPLY (SELECT column_name FROM [dbo].[dv_default_column] WHERE [object_type] = 'CdcSrcMSSQL' AND [object_column_type] = 'CDC_StartLSN') d
OUTER APPLY (SELECT column_name FROM [dbo].[dv_default_column] WHERE [object_type] = 'CdcSrcMSSQL' AND [object_column_type] = 'CDC_Sequence') e
OUTER APPLY (SELECT column_name FROM [dbo].[dv_default_column] WHERE [object_type] = 'CdcStgMSSQL' AND [object_column_type] = 'CDC_Action') f
OUTER APPLY (SELECT column_name FROM [dbo].[dv_default_column] WHERE [object_type] = 'CdcStgMSSQL' AND [object_column_type] = 'CDC_StartLSN') g
OUTER APPLY (SELECT column_name FROM [dbo].[dv_default_column] WHERE [object_type] = 'CdcStgMSSQL' AND [object_column_type] = 'CDC_Sequence') h
 WHERE s.source_unique_name = @source_unique_name

SELECT @SourcePayload += QUOTENAME(c.column_name) + ',' + CHAR(13) + CHAR(10) 
FROM [dbo].[dv_source_system] ss 
INNER JOIN [dbo].[dv_source_table] st ON st.system_key = ss.source_system_key 
INNER JOIN [dbo].[dv_column] c ON c.table_key = st.source_table_key 
WHERE st.source_unique_name = @source_unique_name 
  AND ISNULL(c.is_derived, 0) = 0 
  AND ISNULL(c.is_retired, 0) = 0 
ORDER BY source_ordinal_position 
SELECT @SourcePayload = left(@SourcePayload, len(@SourcePayload) - 3)

IF @load_type = 'MSSQLcdc'
	SET @func_call_full = QUOTENAME(@func_schema_full) + '.' + QUOTENAME(@func_name_full) + ' WHERE (1=1) '
ELSE IF @load_type = 'ODEcdc' 
    SET @func_call_full = QUOTENAME(@func_schema_full) + '.' + QUOTENAME(@func_prefix + @func_name_full + @func_suffix_full) + '(@pit) WHERE (1=1) '
ELSE SET @func_call_full = QUOTENAME(@func_schema_full) + '.' + QUOTENAME(@func_name_full) + ' WHERE (1=1) '

IF @load_type = 'MSSQLcdc'
	SET @func_call_delta = LOWER(QUOTENAME(@func_schema_delta)) + '.' + QUOTENAME(lower(@func_schema_full + '_' + @func_name_full) + @func_suffix_delta)
	    + '(NOLOCK) ' + @crlf + 'WHERE ' + QUOTENAME(@src_cdc_action) + ' IN (1, 2, 4) AND [__$start_lsn] BETWEEN CONVERT(BINARY(10),''@cdc_start_lsn'', 1) AND CONVERT(BINARY(10),''@cdc_end_lsn'', 1)'
ELSE IF @load_type = 'ODEcdc'
    SET @func_call_delta = QUOTENAME(@func_schema_delta) + '.' + QUOTENAME(@func_prefix + @func_name_delta + @func_suffix_delta) + '(@cdc_start_time, @cdc_end_time) WHERE (1=1) '
ELSE SET @func_call_delta = QUOTENAME(@func_schema_full) + '.' + QUOTENAME(@func_name_full) + ' WHERE (1=1) '

SET @func_call_full  = 'SELECT ' + CASE WHEN @load_type IN('ODEcdc', 'MSSQLcdc') THEN @FullSrcTechColList + ',' ELSE '' END + @crlf + @SourcePayload + @crlf + 'FROM ' + @func_call_full
SET @func_call_delta = 'SELECT ' + CASE WHEN @load_type IN('ODEcdc', 'MSSQLcdc') THEN @DeltaSrcTechColList + ',' ELSE '' END + @crlf + @SourcePayload + @crlf + 'FROM ' + @func_call_delta

IF @function_type = 'Full'
	SET @SQL = @func_call_full
ELSE IF @function_type = 'Delta'
	SET @SQL = @func_call_delta
 ELSE SET @SQL = 'Invalid function_type provided'
RETURN @SQL
END