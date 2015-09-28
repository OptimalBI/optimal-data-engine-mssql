CREATE PROC [dv_release].[dv_release_master_insert] 
    @release_number int,
    @release_description varchar(256) = NULL,
    @reference_number varchar(50) = NULL,
    @reference_source varchar(50) = NULL
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN
	
	INSERT INTO [dv_release].[dv_release_master] ([release_number], [release_description], [reference_number], [reference_source])
	SELECT @release_number, @release_description, @reference_number, @reference_source
	-- Begin Return Select <- do not remove
	SELECT [release_key], [release_number], [release_description], [reference_number], [reference_source]
	FROM   [dv_release].[dv_release_master]
	WHERE  [release_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()