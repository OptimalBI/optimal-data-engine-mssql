CREATE PROC [dbo].[dv_hub_insert] 
    @hub_name varchar(128),
    @hub_abbreviation varchar(4) = NULL,
    @hub_schema varchar(128),
    @hub_database varchar(128),
	@is_compressed bit,
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

	INSERT INTO [dbo].[dv_hub] ([hub_name], [hub_abbreviation], [hub_schema], [hub_database],[is_compressed],[is_retired],[release_key])
	SELECT @hub_name, @hub_abbreviation, @hub_schema, @hub_database,@is_compressed,@is_retired,@release_key 
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_hub]
	WHERE  [hub_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()