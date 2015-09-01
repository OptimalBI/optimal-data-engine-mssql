CREATE TABLE [log4].[JournalDetail] (
    [JournalId] INT           NOT NULL,
    [ExtraInfo] VARCHAR (MAX) NULL,
    CONSTRAINT [PK_JournalDetail] PRIMARY KEY NONCLUSTERED ([JournalId] ASC) WITH (FILLFACTOR = 100),
    CONSTRAINT [FK_JournalDetail_Journal] FOREIGN KEY ([JournalId]) REFERENCES [log4].[Journal] ([JournalId]) ON DELETE CASCADE
);

