CREATE TABLE [dv_log].[dv_load_state] (
    [load_state_key]   INT                IDENTITY (1, 1) NOT NULL,
    [source_table_key] INT                DEFAULT ((-1)) NOT NULL,
    [object_key]       INT                DEFAULT ((-1)) NOT NULL,
    [object_type]      VARCHAR (50)       DEFAULT ('<Unknown>') NOT NULL,
    [execution_key]    INT                DEFAULT ((-1)) NOT NULL,
    [load_high_water]  DATETIMEOFFSET (7) NULL,
    [rows_inserted]    INT                DEFAULT ((0)) NOT NULL,
    [rows_updated]     INT                DEFAULT ((0)) NOT NULL,
    [rows_deleted]     INT                DEFAULT ((0)) NOT NULL,
    [updated_by]       VARCHAR (30)       DEFAULT (user_name()) NULL,
    [update_date_time] DATETIMEOFFSET (7) DEFAULT (sysdatetimeoffset()) NOT NULL,
    PRIMARY KEY CLUSTERED ([load_state_key] ASC)
);


GO

CREATE TRIGGER [dv_log].[dv_load_state_audit] ON [dv_log].[dv_load_state]
AFTER INSERT, UPDATE
AS
	BEGIN
	    UPDATE [a]
		 SET
			[update_date_time] = SYSDATETIMEOFFSET()
		   , [updated_by] = SUSER_NAME() FROM [dv_log].[dv_load_state] AS [a]
									   JOIN [inserted] AS [b]
									   ON [a].[load_state_key] = [b].[load_state_key];
	END;