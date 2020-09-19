USE DBA;
CREATE TABLE [dbo].[PartitionsMaintenanceLog]
(
[id] [int] NOT NULL IDENTITY(1, 1),
[insert_time] [datetime] NULL,
[operation] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[database] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[schema] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[table] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[partition_function] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[partition_schema] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[file_group] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[data_file] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[physical_name] [varchar] (2000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[partition_number] [int] NULL,
[partition_boundry] [datetime] NULL,
[rows] [bigint] NULL,
[size_mb] [int] NULL,
[error] [varchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[command] [varchar] (4000) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
CREATE CLUSTERED INDEX [IXC_PartitionsMaintenanceLog__insert_time] ON [dbo].[PartitionsMaintenanceLog] ([insert_time]) WITH (FILLFACTOR=90) ON [PRIMARY]
GO
ALTER TABLE [dbo].[PartitionsMaintenanceLog] ADD CONSTRAINT [PK_PartitionsMaintenanceLog] PRIMARY KEY NONCLUSTERED ([id]) WITH (FILLFACTOR=90) ON [PRIMARY]
GO


CREATE TABLE [dbo].[PartitionsManagment]
(
[id] [int] NOT NULL IDENTITY(1, 1),
[insert_time] [datetime] NULL,
[database] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[partition_function] [varchar] (64) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[days_to_keep_data] [int] NULL,
[num_future_partitions] [int] NULL,
[is_bcp] [bit] NULL,
[bcp_path] [varchar] (1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[partition_boundry_unit] [varchar] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
[create_empty_table] [bit] NULL,
[create_file_group] [bit] NULL,
[is_active] [bit] NULL,
[file_group_physical_path] [varchar] (1024) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
is_split bit NOT NULL,
is_merge bit NOT NULL
) ON [PRIMARY];
GO
ALTER TABLE [dbo].[PartitionsManagment] ADD CONSTRAINT [PK_PartitionsMaintenance] PRIMARY KEY NONCLUSTERED ([id]) WITH (FILLFACTOR=90) ON [PRIMARY]
GO
CREATE UNIQUE NONCLUSTERED INDEX [IXU_database_partition_function] ON [dbo].[PartitionsManagment] ([database], [partition_function]) WITH (FILLFACTOR=90) ON [PRIMARY]
GO
