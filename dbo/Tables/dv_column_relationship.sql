CREATE TABLE [dbo].[dv_column_relationship] (
    [column_relationship_key] INT                IDENTITY (1, 1) NOT NULL,
    [primary_column_key]      INT                NOT NULL,
    [foreign_column_key]      INT                NOT NULL,
    [release_key]             INT                CONSTRAINT [DF_dv_column_relationship_release_key] DEFAULT ((0)) NOT NULL,
    [version_number]          INT                CONSTRAINT [DF__dv_column__versi__13F1F5EB] DEFAULT ((1)) NOT NULL,
    [updated_by]              VARCHAR (30)       CONSTRAINT [DF__dv_column__updat__14E61A24] DEFAULT (user_name()) NOT NULL,
    [updated_datetime]        DATETIMEOFFSET (7) CONSTRAINT [DF__dv_column__updat__15DA3E5D] DEFAULT (sysdatetimeoffset()) NOT NULL,
    CONSTRAINT [PK__dv_colum__DD6110B057F324E6] PRIMARY KEY CLUSTERED ([column_relationship_key] ASC),
    CONSTRAINT [FK__dv_column__dv_column_relationship] FOREIGN KEY ([foreign_column_key]) REFERENCES [dbo].[dv_column] ([column_key]),
    CONSTRAINT [FK_dv_column_relationship_dv_release_master] FOREIGN KEY ([release_key]) REFERENCES [dv_release].[dv_release_master] ([release_key]),
    CONSTRAINT [dv_column_relationship_unique] UNIQUE NONCLUSTERED ([primary_column_key] ASC, [foreign_column_key] ASC)
);

