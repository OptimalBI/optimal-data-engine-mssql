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
    [updated_by]       VARCHAR (30)       CONSTRAINT [DF__dv_defaul__updat__1C873BEC] DEFAULT (user_name()) NULL,
    [updated_datetime] DATETIMEOFFSET (7) CONSTRAINT [DF__dv_defaul__updat__1D7B6025] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_defau__2A343C0024B38F34] PRIMARY KEY CLUSTERED ([default_key] ASC),
    CONSTRAINT [FK_dv_defaults_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [Default_Type_Key] UNIQUE NONCLUSTERED ([default_type] ASC, [default_subtype] ASC)
);



