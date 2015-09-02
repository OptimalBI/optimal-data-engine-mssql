CREATE PROC [dbo].[dv_satellite_insert] 
    @hub_key int,
    @link_key int,
    @link_hub_satellite_flag char(1),
    @satellite_name varchar(128),
    @satellite_abbreviation varchar(4) = NULL,
    @satellite_schema varchar(128),
    @satellite_database varchar(128),
    @is_columnstore bit,
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

	INSERT INTO [dbo].[dv_satellite] ([hub_key], [link_key], [link_hub_satellite_flag], [satellite_name], [satellite_abbreviation], [satellite_schema], [satellite_database], [is_columnstore],[release_key])
	SELECT @hub_key, @link_key, @link_hub_satellite_flag, @satellite_name, @satellite_abbreviation, @satellite_schema, @satellite_database, @is_columnstore, @release_number
	
	-- Begin Return Select <- do not remove
	SELECT [satellite_key], [hub_key], [link_key], [link_hub_satellite_flag], [satellite_name], [satellite_abbreviation], [satellite_schema], [satellite_database], [is_columnstore],[release_key], [version_number], [updated_by], [updated_datetime]
	FROM   [dbo].[dv_satellite]
	WHERE  [satellite_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()