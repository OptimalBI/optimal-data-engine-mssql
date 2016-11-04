CREATE TABLE [dbo].[dv_hub_link_to_be_removed] (
    [hub_link_key]     INT                IDENTITY (1, 1) NOT NULL,
    [link_key]         INT                NOT NULL,
    [hub_key]          INT                NOT NULL,
    [release_key]      INT                NOT NULL,
    [version_number]   INT                NOT NULL,
    [updated_by]       VARCHAR (30)       NULL,
    [updated_datetime] DATETIMEOFFSET (7) NULL
);




GO
