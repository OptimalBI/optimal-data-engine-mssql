CREATE PROC [dbo].[dv_link_key_update] 
    @link_key_column_key int,
    @link_key int,
    @link_key_column_name varchar(128)

AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_link_key_column]
	SET    [link_key] = @link_key, [link_key_column_name] = @link_key_column_name
	WHERE  [link_key_column_key] = @link_key_column_key
	
	-- Begin Return Select <- do not remove
	SELECT [link_key_column_key], [link_key], [link_key_column_name], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_link_key_column]
	WHERE  [link_key_column_key] = @link_key_column_key	
	-- End Return Select <- do not remove

	COMMIT