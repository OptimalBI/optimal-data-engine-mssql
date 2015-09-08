CREATE TABLE [dbo].[dv_satellite_column] (
    [satellite_col_key] INT                IDENTITY (1, 1) NOT NULL,
    [satellite_key]     INT                NOT NULL,
    [column_key]        INT                NOT NULL,
    [release_key]       INT                CONSTRAINT [DF_dv_satellite_column_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]    INT                CONSTRAINT [DF__dv_satell__versi__3CF40B7E] DEFAULT ((1)) NOT NULL,
    [updated_by]        VARCHAR (30)       CONSTRAINT [DF__dv_satell__updat__3DE82FB7] DEFAULT (user_name()) NULL,
    [updated_datetime]  DATETIMEOFFSET (7) CONSTRAINT [DF__dv_satell__updat__3EDC53F0] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_satel__FCBA778F866D59FE] PRIMARY KEY CLUSTERED ([satellite_col_key] ASC),
    CONSTRAINT [FK__dv_satellite_column__dv_column] FOREIGN KEY ([column_key]) REFERENCES [dbo].[dv_column] ([column_key]),
    CONSTRAINT [FK__dv_satellite_column__dv_satellite] FOREIGN KEY ([satellite_key]) REFERENCES [dbo].[dv_satellite] ([satellite_key]),
    CONSTRAINT [FK_dv_satellite_column_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_satellite_column_unique] UNIQUE NONCLUSTERED ([satellite_key] ASC, [column_key] ASC)
);