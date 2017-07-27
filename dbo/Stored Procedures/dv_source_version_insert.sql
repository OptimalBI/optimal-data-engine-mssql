


CREATE PROC [dbo].[dv_source_version_insert] 
    @source_table_key			int,
	@source_version				int,
	@source_type				varchar(50),
	@source_procedure_name		varchar(128),
	@source_filter              nvarchar(max),
    @pass_load_type_to_proc		bit,
	@is_current					bit,
	@release_number				int
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
	
	INSERT INTO [dbo].[dv_source_version] ([source_table_key],[source_version],[source_type],[source_procedure_name],[pass_load_type_to_proc],[source_filter],[is_current],[release_key])
	SELECT @source_table_key, @source_version, @source_type, @source_procedure_name, @pass_load_type_to_proc, @source_filter, @is_current, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_source_version]
	WHERE  [source_version_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()