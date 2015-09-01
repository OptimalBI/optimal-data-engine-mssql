CREATE PROC [dbo].[dv_hub_link_insert] 
    @link_key int,
    @hub_key int,
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

	INSERT INTO [dbo].[dv_hub_link] ([link_key], [hub_key],[release_key])
	SELECT @link_key, @hub_key, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [hub_link_key], [link_key], [hub_key],[release_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_hub_link]
	WHERE  [hub_link_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()

