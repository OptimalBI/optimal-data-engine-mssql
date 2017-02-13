CREATE PROC [dbo].[dv_link_key_insert] 
    @link_key int,
    @link_key_column_name varchar(128),
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

	INSERT INTO [dbo].[dv_link_key_column] ([link_key], [link_key_column_name],[release_key])
	SELECT @link_key, @link_key_column_name, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [link_key_column_key], [link_key], [link_key_column_name],[release_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_link_key_column]
	WHERE  [link_key_column_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()