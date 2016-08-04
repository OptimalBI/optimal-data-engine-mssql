CREATE PROC [dbo].[dv_link_update] 
    @link_key int,
    @link_name varchar(128),
    @link_abbreviation varchar(4) = NULL,
    @link_schema varchar(128),
    @link_database varchar(128),
	@is_retired bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_link]
	SET    [link_name] = @link_name, [link_abbreviation] = @link_abbreviation, [link_schema] = @link_schema, [link_database] = @link_database, [is_retired] = @is_retired
	WHERE  [link_key] = @link_key
	
	-- Begin Return Select <- do not remove
	SELECT [link_key], [link_name], [link_abbreviation], [link_schema], [link_database], [is_retired], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_link]
	WHERE  [link_key] = @link_key	
	-- End Return Select <- do not remove

	COMMIT