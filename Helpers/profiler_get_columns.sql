exec sp_executesql N'
SELECT
--CAST(serverproperty(N''Servername'') AS sysname) AS [Server_Name],
--db_name() AS [Database_Name],
SCHEMA_NAME(tbl.schema_id) AS [Table_Schema],
tbl.name AS [Table_Name],
clmns.column_id AS [ID],
clmns.name AS [Name],
clmns.is_ansi_padded AS [AnsiPaddingStatus],
ISNULL(clmns.collation_name, N'''') AS [Collation],
clmns.column_encryption_key_id AS [ColumnEncryptionKeyID],
clmns.is_computed AS [Computed],
ISNULL(cc.definition,N'''') AS [ComputedText],
s1clmns.name AS [DataTypeSchema],
(case when clmns.default_object_id = 0 then N'''' when d.parent_object_id > 0 then N'''' else d.name end) AS [Default],
ISNULL(dc.Name, N'''') AS [DefaultConstraintName],
(case when clmns.default_object_id = 0 then N'''' when d.parent_object_id > 0 then N'''' else schema_name(d.schema_id) end) AS [DefaultSchema],
clmns.encryption_algorithm_name AS [EncryptionAlgorithm],
CAST(clmns.encryption_type AS int) AS [EncryptionType],
clmns.generated_always_type AS [GeneratedAlwaysType],
ISNULL(clmns.graph_type, 0) AS [GraphType],
clmns.is_identity AS [Identity],
CAST(ISNULL(ic.seed_value,0) AS bigint) AS [IdentitySeed],
CAST(ISNULL(ic.increment_value,0) AS bigint) AS [IdentityIncrement],
CAST(clmns.is_filestream AS bit) AS [IsFileStream],
CAST(ISNULL(cc.is_persisted, 0) AS bit) AS [IsPersisted],
CAST(ISNULL((select TOP 1 1 from sys.foreign_key_columns AS colfk where colfk.parent_column_id = clmns.column_id and colfk.parent_object_id = clmns.object_id), 0) AS bit) AS [IsForeignKey],
CAST(clmns.is_masked AS bit) AS [IsMasked],
CAST(clmns.is_sparse AS bit) AS [IsSparse],
CAST(clmns.is_column_set AS bit) AS [IsColumnSet],
CAST(CASE WHEN baset.name IN (N''nchar'', N''nvarchar'') AND clmns.max_length <> -1 THEN clmns.max_length/2 ELSE clmns.max_length END AS int) AS [Length],
ISNULL(ic.is_not_for_replication, 0) AS [NotForReplication],
clmns.is_nullable AS [Nullable],
CAST(clmns.scale AS int) AS [NumericScale],
CAST(clmns.precision AS int) AS [NumericPrecision],
CAST(clmns.is_rowguidcol AS bit) AS [RowGuidCol],
(case when clmns.rule_object_id = 0 then N'''' else r.name end) AS [Rule],
(case when clmns.rule_object_id = 0 then N'''' else schema_name(r.schema_id) end) AS [RuleSchema],
ISNULL(baset.name, N'''') AS [SystemType],
ISNULL(xscclmns.name, N'''') AS [XmlSchemaNamespace],
ISNULL(s2clmns.name, N'''') AS [XmlSchemaNamespaceSchema],
ISNULL( (case clmns.is_xml_document when 1 then 2 else 1 end), 0) AS [XmlDocumentConstraint],
usrt.name AS [DataType]
FROM
sys.tables AS tbl
INNER JOIN sys.all_columns AS clmns ON clmns.object_id=tbl.object_id
LEFT OUTER JOIN sys.computed_columns AS cc ON cc.object_id = clmns.object_id and cc.column_id = clmns.column_id
LEFT OUTER JOIN sys.types AS usrt ON usrt.user_type_id = clmns.user_type_id
LEFT OUTER JOIN sys.schemas AS s1clmns ON s1clmns.schema_id = usrt.schema_id
LEFT OUTER JOIN sys.objects AS d ON d.object_id = clmns.default_object_id
LEFT OUTER JOIN sys.default_constraints as dc ON clmns.default_object_id = dc.object_id
LEFT OUTER JOIN sys.identity_columns AS ic ON ic.object_id = clmns.object_id and ic.column_id = clmns.column_id
LEFT OUTER JOIN sys.types AS baset ON (baset.user_type_id = clmns.system_type_id and baset.user_type_id = baset.system_type_id) or ((baset.system_type_id = clmns.system_type_id) and (baset.user_type_id = clmns.user_type_id) and (baset.is_user_defined = 0) and (baset.is_assembly_type = 1)) 
LEFT OUTER JOIN sys.objects AS r ON r.object_id = clmns.rule_object_id
LEFT OUTER JOIN sys.xml_schema_collections AS xscclmns ON xscclmns.xml_collection_id = clmns.xml_collection_id
LEFT OUTER JOIN sys.schemas AS s2clmns ON s2clmns.schema_id = xscclmns.schema_id
WHERE
(tbl.name=@_msparam_0 and SCHEMA_NAME(tbl.schema_id)=@_msparam_1)
ORDER BY
--[Database_Name] ASC,
[Table_Schema] ASC,[Table_Name] ASC,[ID] ASC',N'@_msparam_0 nvarchar(4000),@_msparam_1 nvarchar(4000)',@_msparam_0=N'test',@_msparam_1=N'dbo'