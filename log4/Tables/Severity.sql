CREATE TABLE [log4].[Severity] (
    [SeverityId]   INT           NOT NULL,
    [SeverityName] VARCHAR (128) NOT NULL,
    CONSTRAINT [PK_Severity] PRIMARY KEY NONCLUSTERED ([SeverityId] ASC) WITH (FILLFACTOR = 100),
    CONSTRAINT [UQ_Severity_SeverityName] UNIQUE NONCLUSTERED ([SeverityName] ASC) WITH (FILLFACTOR = 100)
);

