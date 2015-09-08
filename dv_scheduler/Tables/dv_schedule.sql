CREATE TABLE [dv_scheduler].[dv_schedule] (
    [schedule_key]         INT                IDENTITY (1, 1) NOT NULL,
    [schedule_name]        VARCHAR (128)      NOT NULL,
    [schedule_description] VARCHAR (256)      NULL,
    [schedule_frequency]   VARCHAR (128)      NOT NULL,
    [release_key]          INT                CONSTRAINT [DF__dv_schedule_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]       INT                CONSTRAINT [DF__dv_schedule__version___534D60F1] DEFAULT ((1)) NOT NULL,
    [updated_by]           VARCHAR (30)       CONSTRAINT [DF__dv_schedule__updated___5441852A] DEFAULT (user_name()) NOT NULL,
    [updated_datetime]     DATETIMEOFFSET (7) CONSTRAINT [DF__dv_schedule__updated___5535A963] DEFAULT (sysdatetimeoffset()) NULL,
    PRIMARY KEY CLUSTERED ([schedule_key] ASC),
    CONSTRAINT [FK_dv_schedule_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key])
);
GO
CREATE UNIQUE NONCLUSTERED INDEX [UX_dv_schedule_schedule_name]
    ON [dv_scheduler].[dv_schedule]([schedule_name] ASC);