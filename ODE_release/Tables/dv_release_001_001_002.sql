CREATE TABLE [ODE_Release].[dv_release_001_001_001]
(
	[release_key] INT NOT NULL PRIMARY KEY IDENTITY, 
    [release_applied_date] DATETIMEOFFSET NULL
)
GO
CREATE INDEX [PK_dv_release_001_001_001_Column] ON [ODE_Release].[dv_release_001_001_001] ([release_key])