CREATE TABLE [dbo].[dv_defaults] (
    [default_key]      INT                IDENTITY (1, 1) NOT NULL,
    [default_type]     VARCHAR (50)       NOT NULL,
    [default_subtype]  VARCHAR (50)       NOT NULL,
    [default_sequence] INT                CONSTRAINT [DF__dv_defaul__defau__19AACF41] DEFAULT ((1)) NOT NULL,
    [data_type]        VARCHAR (50)       CONSTRAINT [DF__dv_defaul__data___1A9EF37A] DEFAULT ('varchar') NOT NULL,
    [default_integer]  INT                NULL,
    [default_varchar]  VARCHAR (128)      NULL,
    [default_dateTime] DATETIME           NULL,
    [release_key]      INT                CONSTRAINT [DF_dv_defaults_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]   INT                CONSTRAINT [DF__dv_defaul__versi__1B9317B3] DEFAULT ((1)) NOT NULL,
    [updated_by]       VARCHAR (128)      CONSTRAINT [DF__dv_defaul__updat__1C873BEC] DEFAULT (suser_name()) NULL,
    [updated_datetime] DATETIMEOFFSET (7) CONSTRAINT [DF__dv_defaul__updat__1D7B6025] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_defau__2A343C0024B38F34] PRIMARY KEY CLUSTERED ([default_key] ASC),
    CONSTRAINT [FK_dv_defaults_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [Default_Type_Key] UNIQUE NONCLUSTERED ([default_type] ASC, [default_subtype] ASC)
);


GO
CREATE TRIGGER [dbo].[dv_defaults_audit]
on [dbo].[dv_defaults]
after insert, update
as
begin
update a
set [updated_datetime] = sysdatetimeoffset(),
    [updated_by]	   = suser_name()
from [dbo].[dv_defaults] as a
join inserted as b 
on a.[default_key] = b.[default_key]; 
end