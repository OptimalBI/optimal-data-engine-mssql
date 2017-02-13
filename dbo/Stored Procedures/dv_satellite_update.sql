CREATE PROC [dbo].[dv_satellite_update] 
    @satellite_key int,
    @hub_key int,
    @link_key int,
    @link_hub_satellite_flag char(1),
    @satellite_name varchar(128),
    @satellite_abbreviation varchar(4) = NULL,
    @satellite_schema varchar(128),
    @satellite_database varchar(128),
	@duplicate_removal_threshold	int,
    @is_columnstore bit,
	@is_compressed bit,
	@is_retired bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_satellite]
	SET    [hub_key] = @hub_key, [link_key] = @link_key, [link_hub_satellite_flag] = @link_hub_satellite_flag, [satellite_name] = @satellite_name, [satellite_abbreviation] = @satellite_abbreviation, [satellite_schema] = @satellite_schema, [satellite_database] = @satellite_database, [duplicate_removal_threshold] = @duplicate_removal_threshold, [is_columnstore] = @is_columnstore, [is_compressed] = @is_compressed, [is_retired] = @is_retired
	WHERE  [satellite_key] = @satellite_key
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_satellite]
	WHERE  [satellite_key] = @satellite_key	
	-- End Return Select <- do not remove

	COMMIT