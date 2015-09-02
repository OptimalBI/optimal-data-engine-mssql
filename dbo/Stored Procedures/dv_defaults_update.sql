CREATE PROC [dbo].[dv_defaults_update] 
    @default_key int,
    @default_type varchar(50),
    @default_subtype varchar(50),
    @default_sequence int,
    @data_type varchar(50),
    @default_integer int = NULL,
    @default_varchar varchar(128) = NULL,
    @default_dateTime datetime = NULL
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_defaults]
	SET    [default_type] = @default_type, [default_subtype] = @default_subtype, [default_sequence] = @default_sequence, [data_type] = @data_type, [default_integer] = @default_integer, [default_varchar] = @default_varchar, [default_dateTime] = @default_dateTime
	WHERE  [default_key] = @default_key
	
	-- Begin Return Select <- do not remove
	SELECT [default_key], [default_type], [default_subtype], [default_sequence], [data_type], [default_integer], [default_varchar], [default_dateTime], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_defaults]
	WHERE  [default_key] = @default_key	
	-- End Return Select <- do not remove

	COMMIT