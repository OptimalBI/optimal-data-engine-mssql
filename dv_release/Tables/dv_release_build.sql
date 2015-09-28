CREATE TABLE [dv_release].[dv_release_build] (
    [release_build_key]          INT           NOT NULL,
    [release_statement_sequence] INT           NOT NULL,
    [release_number]             INT           CONSTRAINT [DF_dv_release_build_release_number] DEFAULT ((0)) NOT NULL,
    [release_statement_type]     VARCHAR (10)  CONSTRAINT [DF_dv_release_build_release_statement_type] DEFAULT ('Header') NOT NULL,
    [release_statement]          VARCHAR (MAX) NULL,
    [affected_row_count]         INT           CONSTRAINT [DF_dv_release_build_affected_row_count] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [PK_dv_release_build] PRIMARY KEY CLUSTERED ([release_build_key] ASC, [release_statement_sequence] ASC)
);