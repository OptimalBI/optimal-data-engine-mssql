
CREATE PROC [dbo].[dv_column_match_insert] 
	  @match_key				int
	 ,@left_hub_key_column_key	int
	 ,@left_link_key_column_key	int
	 ,@left_satellite_col_key	int
	 ,@left_column_key			int
	 ,@right_hub_key_column_key	int
	 ,@right_link_key_column_key int
	 ,@right_satellite_col_key	int
	 ,@right_column_key			int
 	 ,@release_number			int
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

INSERT INTO [dbo].[dv_column_match]
           ([match_key]
           ,[left_hub_key_column_key]
           ,[left_link_key_column_key]
           ,[left_satellite_col_key]
           ,[left_column_key]
           ,[right_hub_key_column_key]
           ,[right_link_key_column_key]
           ,[right_satellite_col_key]
           ,[right_column_key]
           ,[release_key])
     VALUES
           (@match_key
           ,@left_hub_key_column_key
           ,@left_link_key_column_key
           ,@left_satellite_col_key
           ,@left_column_key
           ,@right_hub_key_column_key
           ,@right_link_key_column_key
           ,@right_satellite_col_key
           ,@right_column_key
           ,@release_key)

	-- Begin Return Select <- do not remove
	SELECT *
    FROM [dbo].[dv_column_match]
	WHERE  [col_match_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()