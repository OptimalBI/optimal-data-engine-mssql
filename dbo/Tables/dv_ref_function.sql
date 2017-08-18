CREATE TABLE [dbo].[dv_ref_function] (
    [ref_function_key]  INT                IDENTITY (1, 1) NOT NULL,
    [ref_function_name] VARCHAR (128)      NOT NULL,
    [ref_function]      NVARCHAR (4000)    NOT NULL,
    [is_retired]        BIT                CONSTRAINT [DF_dv_ref_function_is_retired] DEFAULT ((0)) NOT NULL,
    [release_key]       INT                CONSTRAINT [DF_dv_ref_function_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]    INT                CONSTRAINT [DF_dv_ref_function_version] DEFAULT ((1)) NOT NULL,
    [updated_by]        VARCHAR (128)      CONSTRAINT [DF_dv_ref_function_updated] DEFAULT (suser_name()) NULL,
    [updated_datetime]  DATETIMEOFFSET (7) CONSTRAINT [DF_dv_ref_function_updated_datetime] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_ref_function] PRIMARY KEY CLUSTERED ([ref_function_key] ASC),
    CONSTRAINT [FK_dv_ref_function_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key])
);






GO
CREATE UNIQUE NONCLUSTERED INDEX [dv_ref_function_name_unique]
    ON [dbo].[dv_ref_function]([ref_function_name] ASC);


GO

CREATE TRIGGER [dbo].[dv_ref_function_audit] ON [dbo].[dv_ref_function]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[updated_datetime] = SYSDATETIMEOFFSET()
		   , [updated_by] = SUSER_NAME() FROM [dbo].[dv_ref_function] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[ref_function_key] = [b].[ref_function_key];
	END;