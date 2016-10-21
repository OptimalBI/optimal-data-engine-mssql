CREATE TABLE [dbo].[dv_column] (
    [column_key]              INT                IDENTITY (1, 1) NOT NULL,
    [table_key]               INT                NOT NULL,
    [satellite_col_key]       INT                NULL,
    [column_name]             VARCHAR (128)      NOT NULL,
    [column_type]             VARCHAR (30)       NOT NULL,
    [column_length]           INT                NULL,
    [column_precision]        INT                NULL,
    [column_scale]            INT                NULL,
    [Collation_Name]          [sysname]          NULL,
    [bk_ordinal_position]     INT                CONSTRAINT [DF__dv_column__bk_or__31EC6D26] DEFAULT ((0)) NOT NULL,
    [source_ordinal_position] INT                NOT NULL,
    [is_source_date]          BIT                CONSTRAINT [DF__dv_column__is_so__32E0915F] DEFAULT ((0)) NOT NULL,
    [is_retired]              BIT                CONSTRAINT [DF__dv_column__disca__33D4B598] DEFAULT ((0)) NOT NULL,
    [release_key]             INT                CONSTRAINT [DF_dv_column_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]          INT                CONSTRAINT [DF__dv_column__versi__35BCFE0A] DEFAULT ((1)) NOT NULL,
    [updated_by]              VARCHAR (30)       CONSTRAINT [DF__dv_column__updat__36B12243] DEFAULT (suser_name()) NULL,
    [update_date_time]        DATETIMEOFFSET (7) CONSTRAINT [DF__dv_column__updat__37A5467C] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_colum__448C9D1E0C33CF7F] PRIMARY KEY CLUSTERED ([column_key] ASC),
    CONSTRAINT [FK__dv_column__dv_satellite_column] FOREIGN KEY ([satellite_col_key]) REFERENCES [dbo].[dv_satellite_column] ([satellite_col_key]),
    CONSTRAINT [FK__dv_column__dv_source_table] FOREIGN KEY ([table_key]) REFERENCES [dbo].[dv_source_table] ([source_table_key]),
    CONSTRAINT [FK_dv_column_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_column_unique] UNIQUE NONCLUSTERED ([table_key] ASC, [column_name] ASC)
);






GO



GO


CREATE TRIGGER [dbo].[dv_column_audit]
on [dbo].[dv_column]
after insert, update
as
begin
update a
set [update_date_time] = sysdatetimeoffset(),
    [updated_by]	   = suser_name()
from [dbo].[dv_column] as a
join inserted as b 
on a.[column_key] = b.[column_key]; 
end
GO
CREATE UNIQUE NONCLUSTERED INDEX [idx_dv_column_sat_unique]
    ON [dbo].[dv_column]([satellite_col_key] ASC) WHERE ([satellite_col_key] IS NOT NULL);

