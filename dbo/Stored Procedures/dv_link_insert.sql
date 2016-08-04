CREATE PROC [dbo].[dv_link_insert] 
    @link_name varchar(128),
    @link_abbreviation varchar(4) = NULL,
    @link_schema varchar(128),
    @link_database varchar(128),
	@is_retired bit,
	@release_number int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN
	
	declare @release_key int
	       ,@rc int
	select @release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @release_number
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Release Number %i Does Not Exist', 16, 1, @release_number)

	INSERT INTO [dbo].[dv_link] ([link_name], [link_abbreviation], [link_schema], [link_database], [is_retired], [release_key])
	SELECT @link_name, @link_abbreviation, @link_schema, @link_database, @is_retired, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [link_key], [link_name], [link_abbreviation], [link_schema], [link_database], [is_retired], [release_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_link]
	WHERE  [link_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()