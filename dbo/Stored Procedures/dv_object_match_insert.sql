
CREATE PROC [dbo].[dv_object_match_insert] 
    @source_version_key int,
    @temporal_pit_left datetimeoffset(7),
    @temporal_pit_right datetimeoffset(7),
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

INSERT INTO [dbo].[dv_object_match]
           ([source_version_key]
           ,[temporal_pit_left]
           ,[temporal_pit_right]
           ,[is_retired]
           ,[release_key])
     VALUES
           (@source_version_key
           ,@temporal_pit_left
           ,@temporal_pit_right
           ,@is_retired
           ,@release_key)

	-- Begin Return Select <- do not remove
	SELECT *
    FROM [dbo].[dv_object_match]
	WHERE  [match_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()