exec sp_executesql N'SELECT
CAST(
        serverproperty(N''Servername'')
       AS sysname) AS [Server_Name],
db_name() AS [Database_Name],
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
i.name AS [Name],
CAST(ISNULL(k.is_system_named, 0) AS bit) AS [IsSystemNamed],
i.is_primary_key + 2*i.is_unique_constraint AS [IndexKeyType],
CAST(
          CASE i.type WHEN 1 THEN 0 WHEN 4 THEN 4
                      WHEN 3 THEN CASE xi.xml_index_type WHEN 0 THEN 2 WHEN 1 THEN 3 WHEN 2 THEN 7 WHEN 3 THEN 8 END
                      WHEN 4 THEN 4 WHEN 6 THEN 5 WHEN 7 THEN 6 WHEN 5 THEN 9 ELSE 1 END
        AS tinyint) AS [IndexType],
i.is_unique AS [IsUnique],
CAST(CASE i.index_id WHEN 1 THEN 1 ELSE 0 END AS bit) AS [IsClustered],
i.is_disabled AS [IsDisabled],
CASE WHEN ''PS''=dsi.type THEN dsi.name ELSE N'''' END AS [PartitionScheme],
case UPPER(ISNULL(xi.secondary_type,'''')) when ''P'' then 1 when ''V'' then 2 when ''R'' then 3 else 0 end AS [SecondaryXmlIndexType],

        CASE WHEN ((SELECT tbli.is_memory_optimized FROM sys.tables tbli WHERE tbli.object_id = i.object_id)=1 or
        (SELECT tti.is_memory_optimized FROM sys.table_types tti WHERE tti.type_table_object_id = i.object_id)=1)
        THEN ISNULL((SELECT ds.name FROM sys.data_spaces AS ds WHERE ds.type=''FX''), N'''')
        ELSE CASE WHEN ''FG''=dsi.type THEN dsi.name ELSE N'''' END
        END
       AS [FileGroup],
CASE WHEN ''FD''=dstbl.type THEN dstbl.name ELSE N'''' END AS [FileStreamFileGroup],
CASE WHEN ''PS''=dstbl.type THEN dstbl.name ELSE N'''' END AS [FileStreamPartitionScheme],
i.ignore_dup_key AS [IgnoreDuplicateKeys],
i.fill_factor AS [FillFactor],
CAST(ISNULL(INDEXPROPERTY(i.object_id, i.name, N''IsPadIndex''), 0) AS bit) AS [PadIndex],
~i.allow_row_locks AS [DisallowRowLocks],
~i.allow_page_locks AS [DisallowPageLocks],
ISNULL(s.no_recompute,0) AS [NoAutomaticRecomputation],
CAST(ISNULL(spi.spatial_index_type,0) AS tinyint) AS [SpatialIndexType],
CAST(ISNULL(si.bounding_box_xmin,0) AS float(53)) AS [BoundingBoxXMin],
CAST(ISNULL(si.bounding_box_ymin,0) AS float(53)) AS [BoundingBoxYMin],
CAST(ISNULL(si.bounding_box_xmax,0) AS float(53)) AS [BoundingBoxXMax],
CAST(ISNULL(si.bounding_box_ymax,0) AS float(53)) AS [BoundingBoxYMax],
CAST(ISNULL(si.level_1_grid,0) AS smallint) AS [Level1Grid],
CAST(ISNULL(si.level_2_grid,0) AS smallint) AS [Level2Grid],
CAST(ISNULL(si.level_3_grid,0) AS smallint) AS [Level3Grid],
CAST(ISNULL(si.level_4_grid,0) AS smallint) AS [Level4Grid],
CAST(ISNULL(si.cells_per_object,0) AS int) AS [CellsPerObject],
ISNULL(i.filter_definition, N'''') AS [FilterDefinition],
CAST(OBJECTPROPERTY(i.object_id,N''IsMSShipped'') AS bit) AS [IsSystemObject],
CAST(CASE WHEN filetableobj.object_id IS NULL THEN 0 ELSE 1 END AS bit) AS [IsFileTableDefined],
CAST(i.compression_delay AS int) AS [CompressionDelay]
FROM
sys.tables AS tbl
INNER JOIN sys.indexes AS i ON (i.index_id > @_msparam_0 and i.is_hypothetical = @_msparam_1) AND (i.object_id=tbl.object_id)
LEFT OUTER JOIN sys.key_constraints AS k ON k.parent_object_id = i.object_id AND k.unique_index_id = i.index_id
LEFT OUTER JOIN sys.xml_indexes AS xi ON xi.object_id = i.object_id AND xi.index_id = i.index_id
LEFT OUTER JOIN sys.data_spaces AS dsi ON dsi.data_space_id = i.data_space_id
LEFT OUTER JOIN sys.tables AS t ON t.object_id = i.object_id
LEFT OUTER JOIN sys.data_spaces AS dstbl ON dstbl.data_space_id = t.Filestream_data_space_id and (i.index_id < 2 or (i.type = 7 and i.index_id < 3))
LEFT OUTER JOIN sys.stats AS s ON s.stats_id = i.index_id AND s.object_id = i.object_id
LEFT OUTER JOIN sys.spatial_indexes AS spi ON i.object_id = spi.object_id and i.index_id = spi.index_id
LEFT OUTER JOIN sys.spatial_index_tessellations as si ON i.object_id = si.object_id and i.index_id = si.index_id
LEFT OUTER JOIN sys.filetable_system_defined_objects AS filetableobj ON i.object_id = filetableobj.object_id
WHERE
(tbl.name=@_msparam_2 and SCHEMA_NAME(tbl.schema_id)=@_msparam_3)
ORDER BY
[Database_Name] ASC,[Table_Schema] ASC,[Table_Name] ASC,[Name] ASC',N'@_msparam_0 nvarchar(4000),@_msparam_1 nvarchar(4000),@_msparam_2 nvarchar(4000),@_msparam_3 nvarchar(4000)',@_msparam_0=N'0',@_msparam_1=N'0',@_msparam_2=N'test',@_msparam_3=N'dbo'