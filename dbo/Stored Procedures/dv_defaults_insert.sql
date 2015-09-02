CREATE PROC [dbo].[dv_defaults_insert] 
    @default_type varchar(50),
    @default_subtype varchar(50),
    @default_sequence int,
    @data_type varchar(50),
    @default_integer int = NULL,
    @default_varchar varchar(128) = NULL,
    @default_dateTime datetime = NULL,
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
	INSERT INTO [dbo].[dv_defaults] ([default_type], [default_subtype], [default_sequence], [data_type], [default_integer], [default_varchar], [default_dateTime],[release_key])
	SELECT @default_type, @default_subtype, @default_sequence, @data_type, @default_integer, @default_varchar, @default_dateTime, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [default_key], [default_type], [default_subtype], [default_sequence], [data_type], [default_integer], [default_varchar], [default_dateTime],[release_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_defaults]
	WHERE  [default_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()