CREATE TABLE [log4].[Journal] (
    [JournalId]         INT            IDENTITY (1, 1) NOT NULL,
    [UtcDate]           DATETIME       CONSTRAINT [DF_Journal_UtcDate] DEFAULT (getutcdate()) NULL,
    [SystemDate]        DATETIME       CONSTRAINT [DF_Journal_SystemDate] DEFAULT (getdate()) NULL,
    [Task]              VARCHAR (128)  CONSTRAINT [DF_Journal_Task] DEFAULT ('') NULL,
    [FunctionName]      VARCHAR (256)  NULL,
    [StepInFunction]    VARCHAR (128)  NULL,
    [MessageText]       VARCHAR (512)  NULL,
    [SeverityId]        INT            NULL,
    [ExceptionId]       INT            NULL,
    [SessionId]         INT            NULL,
    [ServerName]        NVARCHAR (128) NULL,
    [DatabaseName]      NVARCHAR (128) NULL,
    [HostName]          NVARCHAR (128) NULL,
    [ProgramName]       NVARCHAR (128) NULL,
    [NTDomain]          NVARCHAR (128) NULL,
    [NTUsername]        NVARCHAR (128) NULL,
    [LoginName]         NVARCHAR (128) NULL,
    [OriginalLoginName] NVARCHAR (128) NULL,
    [SessionLoginTime]  DATETIME       NULL,
    CONSTRAINT [PK_Journal] PRIMARY KEY NONCLUSTERED ([JournalId] ASC) WITH (FILLFACTOR = 100)
);



