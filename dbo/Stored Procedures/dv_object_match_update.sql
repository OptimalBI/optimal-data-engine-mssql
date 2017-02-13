


CREATE PROC [dbo].[dv_object_match_update] 
      @match_key			int
	 ,@source_version_key   int
	 ,@temporal_pit_left	datetimeoffset(7)
	 ,@temporal_pit_right	datetimeoffset(7)
	 ,@is_retired			bit

AS 
SET NOCOUNT ON 
SET XACT_ABORT ON 	
BEGIN TRAN

    UPDATE [dbo].[dv_object_match]
    SET [source_version_key]	= @source_version_key
	   ,[temporal_pit_left]		= @temporal_pit_left	
       ,[temporal_pit_right]	= @temporal_pit_right	
       ,[is_retired]			= @is_retired			
 	WHERE [match_key] = @match_key
	
	-- Begin Return Select <- do not remove
SELECT *
  FROM [dbo].[dv_object_match]
  WHERE  [match_key] = @match_key	
	-- End Return Select <- do not remove
COMMIT