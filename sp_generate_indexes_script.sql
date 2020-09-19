/*
--Example:

DECLARE @command varchar(MAX), @database sysname = DB_NAME(), @schema sysname = 'dbo', @table sysname = 'cnv_ccs_click_daily', @empty_table sysname = 'new_table', @file_group sysname = 'FGGGGG', @partition_number_int int = 1
EXEC dbo.sp_generate_indexes_script @DBName = @database, @SchemaName	= @schema, @TableName = @table, @NewTableName = @empty_table, @FileGroup = @file_group, @PartitionNumber = @partition_number_int, @IndexesCommand = @command OUTPUT;
PRINT @command;

*/						

USE master; 
SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;
IF OBJECT_ID('dbo.sp_generate_indexes_script', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_generate_indexes_script;
GO
/*
	20160504 Yaniv Etrogi
	Added SUBSTRING to the index name to limit the length of the name to 128 which is the max possible len 
	and remove the QUOTNAME function that failed because the input string(index.table) was too long
*/
CREATE PROCEDURE dbo.sp_generate_indexes_script
  @DBName sysname
 ,@SchemaName sysname
 ,@TableName sysname
 ,@NewTableName sysname
 ,@FileGroup sysname
 ,@PartitionNumber int
 ,@IndexesCommand nvarchar(MAX) OUTPUT
AS
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
--DECLARE @SchemaName sysname  = 'dbo', @TableName sysname = 'f_impressions', @PartitionNumber int = 1, @NewTableName sysname = 'new_table', @FileGroup sysname = 'FGGGG',@PartitionNumber int;

IF OBJECT_ID('tempdb.dbo.#indexes', 'U') IS NOT NULL DROP TABLE dbo.#indexes ;
CREATE TABLE dbo.#indexes 
(
	id int IDENTITY(1,1) PRIMARY KEY,
	[object_id] [int] NOT NULL,
	[index_name] [sysname] NULL,
	[index_id] [int] NOT NULL,
	[type] [tinyint] NOT NULL,
	[type_desc] [nvarchar](60) NULL,
	[is_unique] [bit] NULL,
	[data_space_id] [int] NULL,
	[ignore_dup_key] [bit] NULL,
	[is_primary_key] [bit] NULL,
	[is_unique_constraint] [bit] NULL,
	[fill_factor] [tinyint] NOT NULL,
	[is_padded] [bit] NULL,
	[is_disabled] [bit] NULL,
	[is_hypothetical] [bit] NULL,
	[allow_row_locks] [bit] NULL,
	[allow_page_locks] [bit] NULL,
	[has_filter] [bit] NULL,
	[filter_definition] [nvarchar](MAX) NULL,
	[key_ordinal] [tinyint] NOT NULL,
	[partition_ordinal] [tinyint] NOT NULL,
	[is_descending_key] [bit] NULL,
	[is_included_column] [bit] NULL,
	[column_name] [sysname] NULL,
	auto_created bit NULL,
	no_recompute bit NULL,
	partition_number int NULL,
	data_compression_desc sysname NULL,
	included_columns varchar(8000) NULL
);

-- Get all indexes information
INSERT dbo.#indexes
		(object_id
		,index_name
		,index_id
		,type
		,type_desc
		,is_unique
		,data_space_id
		,ignore_dup_key
		,is_primary_key
		,is_unique_constraint
		,fill_factor
		,is_padded
		,is_disabled
		,is_hypothetical
		,allow_row_locks
		,allow_page_locks
		,has_filter
		,filter_definition
		,key_ordinal
		,partition_ordinal
		,is_descending_key
		,is_included_column
		,column_name
		,auto_created
		,no_recompute
		,partition_number
		,data_compression_desc
		,included_columns)
SELECT
	  i.object_id
	 ,i.name AS index_name
	 ,i.index_id
	 ,i.type
	 ,i.type_desc
	 ,i.is_unique
	 ,i.data_space_id
	 ,i.ignore_dup_key
	 ,i.is_primary_key
	 ,i.is_unique_constraint
	 ,i.fill_factor
	 ,i.is_padded
	 ,i.is_disabled
	 ,i.is_hypothetical
	 ,i.allow_row_locks
	 ,i.allow_page_locks
	 ,i.has_filter
	 ,i.filter_definition
	 ,ic.key_ordinal
	 ,ic.partition_ordinal
	 ,ic.is_descending_key
	 ,ic.is_included_column
	,c.name AS column_name
	,s.auto_created
	,s.no_recompute
	,p.partition_number
	,P.data_compression_desc
	,SUBSTRING(list, indCol.splitter + 1, 4000) COLLATE DATABASE_DEFAULT AS included_columns
FROM sys.indexes i
INNER JOIN sys.index_columns ic ON ic.index_id = i.index_id AND ic.object_id = i.object_id
INNER JOIN sys.columns c ON c.column_id = ic.column_id AND c.object_id = i.object_id 
INNER JOIN sys.stats s ON s.object_id = i.object_id AND s.stats_id = i.index_id
INNER JOIN sys.partitions p ON p.index_id = i.index_id AND p.object_id = i.object_id
INNER JOIN sys.tables t ON t.object_id = i.object_id
CROSS APPLY (
							SELECT NULLIF(CHARINDEX('|', indexCols.list), 0) splitter ,list
							FROM
								(
									SELECT CAST((SELECT CASE WHEN sc.is_included_column = 1 AND sc.ColPos = 1 THEN '|' ELSE '' END + CASE WHEN sc.ColPos > 1 THEN ', ' ELSE '' END + name
									FROM
										(
											SELECT 
												ROW_NUMBER() OVER (PARTITION BY ic.is_included_column ORDER BY ic.key_ordinal) ColPos,
												ic.is_included_column, ic.index_column_id, c.name
											FROM 
												sys.index_columns ic
												INNER JOIN sys.columns c ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
											WHERE 
												ic.index_id = i.index_id AND 
												ic.[object_id] = i.[object_id] AND 
												ic.key_ordinal = CASE WHEN (ic.key_ordinal = 0 AND ic.is_included_column = 0) THEN 1 ELSE ic.key_ordinal END
										) sc
									ORDER BY sc.is_included_column, ColPos FOR XML PATH('') ,TYPE ) AS VARCHAR(MAX)) list
								) indexCols
						) indCol
WHERE @SchemaName = SCHEMA_NAME(t.schema_id) AND t.name = @TableName
AND i.is_hypothetical = 0 AND i.is_disabled = 0
--Yaniv Etrogi 20161025 - Only clsutered indexes are needed for the switch out
AND i.index_id = 1
--SELECT * FROM #indexes


--Get the number of columns per index required for the inner loop that constructs the @index_keys
IF OBJECT_ID('tempdb.dbo.#indexes_keys', 'U') IS NOT NULL DROP TABLE dbo.#indexes_keys;
CREATE TABLE #indexes_keys(id int IDENTITY(1,1), index_id int, key_ordinal int);
INSERT dbo.#indexes_keys (index_id, key_ordinal) 
SELECT MAX(index_id), MAX(key_ordinal) FROM dbo.#indexes GROUP BY index_id ORDER BY index_id;
--SELECT * FROM #indexes_keys;


DECLARE @index_options varchar(max), @index_keys varchar(max);
DECLARE @index_id int, @key_ordinal int, @current_key_ordinal int, @id int, @max_id int, @is_primary_key bit; 
DECLARE @file_group_name sysname, @data_space_id int;


SELECT @current_key_ordinal = 1, @id = 1, @max_id = (SELECT MAX(id) FROM dbo.#indexes_keys);
SELECT @index_id = index_id, @key_ordinal = key_ordinal FROM #indexes_keys WHERE id = @id;

WHILE @id <= @max_id
BEGIN;
			--PRINT '-- id: ' + CAST(@id AS sysname) + ' | index_id: ' + CAST(@index_id AS sysname) + ' | key_ordinal: ' + CAST(@key_ordinal AS sysname) + ' | current_key_ordinal: ' + CAST(@current_key_ordinal AS sysname);
		
			-- index_options 
			SELECT 
					 @is_primary_key = is_primary_key
					,@data_space_id = data_space_id
					,@index_options = 
						CASE WHEN is_padded = 1 THEN 'PAD_INDEX = ON, ' ELSE 'PAD_INDEX = OFF, ' END 
					+ CASE WHEN [allow_page_locks] = 1 THEN 'ALLOW_PAGE_LOCKS = ON, ' ELSE 'ALLOW_PAGE_LOCKS = OFF, ' END 
					+ CASE WHEN allow_row_locks = 1 THEN 'ALLOW_ROW_LOCKS = ON, ' ELSE 'ALLOW_ROW_LOCKS = OFF, ' END 
					+ CASE WHEN INDEXPROPERTY(object_id, index_name, 'IsStatistics') = 1 THEN 'STATISTICS_NORECOMPUTE = ON, ' ELSE 'STATISTICS_NORECOMPUTE = OFF, ' END 
					+ CASE WHEN [ignore_dup_key] = 1 THEN 'IGNORE_DUP_KEY = ON, ' ELSE 'IGNORE_DUP_KEY = OFF, ' END 
					+ 'SORT_IN_TEMPDB = OFF, DATA_COMPRESSION = ' + data_compression_desc + ', FILLFACTOR = ' + CASE WHEN fill_factor = 0 THEN '100' ELSE CAST(fill_factor AS varchar(3)) END 
			FROM dbo.#indexes 
			WHERE partition_number = @PartitionNumber AND index_id = @index_id;

				
				-- index_keys
				WHILE @current_key_ordinal <= @key_ordinal
				BEGIN;

						--PRINT '-- current_key_ordinal: ' + CAST(@current_key_ordinal AS sysname);
						SELECT TOP 1
							 @index_keys = ISNULL(@index_keys, '') + ', ' + CASE WHEN is_descending_key = 1 THEN column_name + ' DESC ' ELSE column_name END
						FROM dbo.#indexes
						WHERE index_id = @index_id AND key_ordinal = @current_key_ordinal;

						SELECT @current_key_ordinal += 1;
				END;

				--Remove the first comma
				SELECT @index_keys = RIGHT(@index_keys, LEN(@index_keys)-PATINDEX('%,%', @index_keys) );

				--PRINT '@SchemaName: ' + @SchemaName + ' | @NewTableName: ' + @NewTableName + ' | @index_options: ' + @index_options + ' | @index_keys: ' + @index_keys;

				-- Primary Key
				IF @is_primary_key = 1
				BEGIN;
					-- Get the file_group name
					--SELECT @file_group_name = dbo.get_file_group_name_by_data_space_id(@data_space_id, @PartitionNumber, @index_id);

					SELECT TOP 1 @IndexesCommand = ISNULL(@IndexesCommand, '') + 
							 'ALTER TABLE [' + @SchemaName + '].[' + @NewTableName + '] ADD CONSTRAINT [' + index_name + '_Empty] PRIMARY KEY ' + CASE WHEN type_desc IN ('CLUSTERED', 'NONCLUSTERED') THEN type_desc ELSE '' END +
							 ' (' + @index_keys + ')' + CHAR(10)
							+ CASE WHEN included_columns IS NOT NULL THEN ' INCLUDE (' + included_columns + ')' + CHAR(10) ELSE '' END
							+ CASE WHEN has_filter = 1 THEN ' WHERE ' + filter_definition + CHAR(10) ELSE '' END
							+ 'WITH (' + @index_options + ')' + CHAR(10) 
							+ 'ON [' + @FileGroup + '];' + CHAR(10)
					FROM dbo.#indexes 
					WHERE partition_number = @PartitionNumber AND index_id = @index_id;
				END;

				-- Indexes
				IF @is_primary_key = 0
				BEGIN;
					 -- Get the file_group name
					 --SELECT @file_group_name = dbo.get_file_group_name_by_data_space_id(@data_space_id, @PartitionNumber, @index_id);
					-- PRINT @IndexesCommand
					 
					 SELECT TOP 1 @IndexesCommand = ISNULL(@IndexesCommand, '') + 
						 'CREATE ' + CASE WHEN is_unique = 1 THEN 'UNIQUE ' ELSE '' END + CASE WHEN type_desc IN ('CLUSTERED', 'NONCLUSTERED') THEN type_desc ELSE '' END + ' INDEX [' + SUBSTRING(index_name + '_' + @NewTableName, 0, 128) + '] ON [' + @SchemaName + '].[' + @NewTableName + '] (' + @index_keys + ')' + CHAR(10)
						+ CASE WHEN included_columns IS NOT NULL THEN ' INCLUDE (' + included_columns + ')' + CHAR(10) ELSE '' END
						+ CASE WHEN has_filter = 1 THEN ' WHERE ' + filter_definition + CHAR(10) ELSE '' END
						+ 'WITH (' + @index_options + ')' + CHAR(10) 
						+ 'ON [' + @FileGroup  + '];' + CHAR(10)
					FROM #indexes
					WHERE partition_number = @PartitionNumber AND index_id = @index_id;
				END;

				--SELECT * FROM dbo.#indexes WHERE partition_number = @PartitionNumber AND index_id = @index_id;
				--PRINT '-- @id: ' + CAST(@id AS sysname)

		--Increment the outer loop id
		SELECT @id += 1;
		-- Reset variables for the next iteration
		SELECT @current_key_ordinal = 1, @index_options = NULL, @index_keys = NULL;
		-- Asign variables for the next iteration
		SELECT @index_id = index_id, @key_ordinal = key_ordinal FROM #indexes_keys WHERE id = @id; 
END;
GO

EXEC sp_MS_marksystemobject 'sp_generate_indexes_script';


