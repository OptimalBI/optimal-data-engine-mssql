CREATE TABLE [dbo].[dv_hub_link] (
    [hub_link_key]     INT                IDENTITY (1, 1) NOT NULL,
    [link_key]         INT                NOT NULL,
    [hub_key]          INT                NOT NULL,
    [release_key]      INT                CONSTRAINT [DF_dv_hub_link_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]   INT                CONSTRAINT [DF__dv_hub_li__versi__373B3228] DEFAULT ((1)) NOT NULL,
    [updated_by]       VARCHAR (30)       CONSTRAINT [DF__dv_hub_li__updat__382F5661] DEFAULT (user_name()) NULL,
    [updated_datetime] DATETIMEOFFSET (7) CONSTRAINT [DF__dv_hub_li__updat__39237A9A] DEFAULT (sysdatetimeoffset()) NULL,
    CONSTRAINT [PK__dv_hub_l__46516BCBEC5E1CB0] PRIMARY KEY CLUSTERED ([hub_link_key] ASC),
    CONSTRAINT [FK__dv_hub_link__dv_hub] FOREIGN KEY ([hub_key]) REFERENCES [dbo].[dv_hub] ([hub_key]),
    CONSTRAINT [FK__dv_hub_link__dv_link] FOREIGN KEY ([link_key]) REFERENCES [dbo].[dv_link] ([link_key]),
    CONSTRAINT [FK_dv_hub_link_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_hub_link_unique] UNIQUE NONCLUSTERED ([link_key] ASC, [hub_key] ASC)
);



