CREATE TABLE [ODE_Release].[dv_release_004_001_001] (
    [release_key]          INT                IDENTITY (1, 1) NOT NULL,
    [release_applied_date] DATETIMEOFFSET (7) NULL,
    PRIMARY KEY CLUSTERED ([release_key] ASC)
);

