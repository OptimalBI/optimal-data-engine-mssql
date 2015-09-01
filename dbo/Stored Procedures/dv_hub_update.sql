CREATE PROC [dbo].[dv_hub_update] 
    @hub_key int,
    @hub_name varchar(128),
    @hub_abbreviation varchar(4) = NULL,
    @hub_schema varchar(128),
    @hub_database varchar(128)
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_hub]
	SET    [hub_name] = @hub_name, [hub_abbreviation] = @hub_abbreviation, [hub_schema] = @hub_schema, [hub_database] = @hub_database
	WHERE  [hub_key] = @hub_key
	
	-- Begin Return Select <- do not remove
	SELECT [hub_key], [hub_name], [hub_abbreviation], [hub_schema], [hub_database]
	FROM   [dbo].[dv_hub]
	WHERE  [hub_key] = @hub_key	
	-- End Return Select <- do not remove

	COMMIT
