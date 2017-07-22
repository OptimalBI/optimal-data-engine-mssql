CREATE TABLE [dbo].[dv_default_column] (
    [default_column_key] INT                IDENTITY (1, 1) NOT NULL,
    [object_type]        VARCHAR (30)       NOT NULL,
    [object_column_type] VARCHAR (30)       NOT NULL,
    [ordinal_position]   INT                CONSTRAINT [DF__dv_defaul__ordin__214BF109] DEFAULT ((0)) NOT NULL,
    [column_prefix]      VARCHAR (30)       NULL,
    [column_name]        VARCHAR (256)      NOT NULL,
    [column_suffix]      VARCHAR (30)       NULL,
    [column_type]        VARCHAR (30)       NOT NULL,
    [column_length]      INT                NULL,
    [column_precision]   INT                NULL,
    [column_scale]       INT                NULL,
    [collation_Name]     [sysname]          NULL,
    [is_nullable]        BIT                CONSTRAINT [DF__dv_defaul__is_nu__22401542] DEFAULT ((1)) NOT NULL,
    [is_pk]              BIT                CONSTRAINT [DF__dv_defaul__is_pk__2334397B] DEFAULT ((0)) NOT NULL,
    [discard_flag]       BIT                CONSTRAINT [DF__dv_defaul__disca__24285DB4] DEFAULT ((0)) NOT NULL,
    [release_key]        INT                CONSTRAINT [DF_dv_default_column_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]     INT                CONSTRAINT [DF__dv_defaul__versi__251C81ED] DEFAULT ((1)) NOT NULL,
    [updated_by]         VARCHAR (128)      CONSTRAINT [DF__dv_defaul__updat__2610A626] DEFAULT (suser_name()) NULL,
    [update_date_time]   DATETIMEOFFSET (7) CONSTRAINT [DF__dv_defaul__updat__2704CA5F] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_defau__56A78F2CAD0ED7B9] PRIMARY KEY CLUSTERED ([default_column_key] ASC),
    CONSTRAINT [FK_dv_default_column_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_default_column_unique] UNIQUE NONCLUSTERED ([object_type] ASC, [column_name] ASC)
);


GO
CREATE TRIGGER [dbo].[dv_default_column_audit]
on [dbo].[dv_default_column]
after insert, update
as
begin
update a
set [update_date_time] = sysdatetimeoffset(),
    [updated_by]	   = suser_name()
from [dbo].[dv_default_column] as a
join inserted as b 
on a.[default_column_key] = b.[default_column_key]; 
end