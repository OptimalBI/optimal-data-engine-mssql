CREATE TABLE [dbo].[dv_connection] (
    [connection_key]      INT                IDENTITY (1, 1) NOT NULL,
    [connection_name]     VARCHAR (50)       NOT NULL,
    [connection_string]   VARCHAR (512)      NOT NULL,
    [connection_password] VARCHAR (128)      NULL,
    [version_number]      INT                CONSTRAINT [DF__dv_connection__version_number] DEFAULT ((1)) NOT NULL,
    [updated_by]          VARCHAR (30)       CONSTRAINT [DF__dv_connection__updated_by] DEFAULT (suser_name()) NULL,
    [updated_datetime]    DATETIMEOFFSET (7) CONSTRAINT [DF__dv_connection__updated_datetime] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_connection] PRIMARY KEY CLUSTERED ([connection_key] ASC),
    CONSTRAINT [dv_connection_connection_name] UNIQUE NONCLUSTERED ([connection_name] ASC)
);


GO

CREATE TRIGGER [dbo].[dv_connection_audit] ON [dbo].[dv_connection]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[updated_datetime]	= SYSDATETIMEOFFSET()			
		   ,[version_number] += 1
		   ,[updated_by]		= SUSER_NAME() 
		 FROM [dbo].[dv_connection] AS [a]
		 JOIN [inserted] AS [b] ON [a].[connection_key] = [b].[connection_key];
	END;