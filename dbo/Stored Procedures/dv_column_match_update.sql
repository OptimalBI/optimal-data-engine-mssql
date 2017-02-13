
CREATE PROC [dbo].[dv_column_match_update]
      @col_match_key			int
     ,@match_key				int
	 ,@left_hub_key_column_key	int
	 ,@left_link_key_column_key	int
	 ,@left_satellite_col_key	int
	 ,@left_column_key			int
	 ,@right_hub_key_column_key	int
	 ,@right_link_key_column_key int
	 ,@right_satellite_col_key	int
	 ,@right_column_key			int

AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON 	
	BEGIN TRAN

UPDATE [dbo].[dv_column_match]
   SET [match_key]						= @match_key
      ,[left_hub_key_column_key]		= @left_hub_key_column_key
      ,[left_link_key_column_key]		= @left_link_key_column_key
      ,[left_satellite_col_key]			= @left_satellite_col_key
      ,[left_column_key]				= @left_column_key
      ,[right_hub_key_column_key]		= @right_hub_key_column_key
      ,[right_link_key_column_key]		= @right_link_key_column_key
      ,[right_satellite_col_key]		= @right_satellite_col_key
      ,[right_column_key]				= @right_column_key
	WHERE  [col_match_key] = @col_match_key
	
	-- Begin Return Select <- do not remove
	SELECT *
    FROM [dbo].[dv_column_match]
	WHERE  [col_match_key] = @col_match_key	
	-- End Return Select <- do not remove

	COMMIT