-- DW diagnostics script
-- Run this in SSMS or with sqlcmd to collect information about the DW structure and content.
-- Save output and paste here if you want me to analyze results.

USE [DWOpinionClientes];
GO

PRINT '--- Database and basic info ---';
SELECT DB_NAME() AS DatabaseName, suser_sname() AS CurrentUser, @@VERSION AS SqlServerVersion;
GO

PRINT '--- Schemas ---';
SELECT s.name AS SchemaName, COUNT(t.object_id) AS TableCount
FROM sys.schemas s
LEFT JOIN sys.tables t ON t.schema_id = s.schema_id
GROUP BY s.name
ORDER BY s.name;
GO

PRINT '--- Tables in Dimension and Fact schemas and row counts ---';
SELECT
    SCHEMA_NAME(t.schema_id) AS SchemaName,
    t.name AS TableName,
    SUM(p.row_count) AS RowCount
FROM sys.tables t
LEFT JOIN sys.dm_db_partition_stats p ON p.object_id = t.object_id AND p.index_id IN (0,1)
WHERE SCHEMA_NAME(t.schema_id) IN ('Dimension','Fact','Staging')
GROUP BY t.schema_id, t.name
ORDER BY SchemaName, TableName;
GO

PRINT '--- Check if staging table exists and sample rows ---';
IF OBJECT_ID('Staging.StgOpiniones','U') IS NOT NULL
BEGIN
    SELECT TOP 5 * FROM Staging.StgOpiniones ORDER BY StgId;
    SELECT COUNT(*) AS StagingRowCount FROM Staging.StgOpiniones;
END
ELSE
BEGIN
    PRINT 'Staging.StgOpiniones does not exist.';
END
GO

PRINT '--- Identity columns (is_identity) for Dimension tables ---';
SELECT
    s.name AS SchemaName,
    t.name AS TableName,
    c.name AS ColumnName,
    c.is_identity
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = 'Dimension'
ORDER BY t.name, c.column_id;
GO

PRINT '--- Foreign keys referencing Dimension tables ---';
SELECT fk.name AS FKName,
       OBJECT_SCHEMA_NAME(fk.parent_object_id) AS ParentSchema,
       OBJECT_NAME(fk.parent_object_id) AS ParentTable,
       OBJECT_SCHEMA_NAME(fk.referenced_object_id) AS RefSchema,
       OBJECT_NAME(fk.referenced_object_id) AS RefTable
FROM sys.foreign_keys fk
WHERE OBJECT_SCHEMA_NAME(fk.referenced_object_id) = 'Dimension'
ORDER BY RefTable, FKName;
GO

PRINT '--- Check business-key duplicates ---';
PRINT 'DimProducto: Total vs Distinct Id_Producto_Origen';
SELECT COUNT(*) AS TotalRows, COUNT(DISTINCT TRY_CAST(Id_Producto_Origen AS NVARCHAR(50))) AS DistinctKeys FROM Dimension.DimProducto;
PRINT 'DimSentimiento: Total vs Distinct Clasificacion';
SELECT COUNT(*) AS TotalRows, COUNT(DISTINCT Clasificacion) AS DistinctKeys FROM Dimension.DimSentimiento;
PRINT 'DimTiempo: Total vs Distinct Fecha';
SELECT COUNT(*) AS TotalRows, COUNT(DISTINCT Fecha) AS DistinctKeys FROM Dimension.DimTiempo;
GO

PRINT '--- Top 5 rows from each Dimension (for spot check) ---';
SELECT TOP 5 * FROM Dimension.DimCliente ORDER BY Cliente_id;
SELECT TOP 5 * FROM Dimension.DimProducto ORDER BY Producto_id;
SELECT TOP 5 * FROM Dimension.DimFuente ORDER BY Fuente_id;
SELECT TOP 5 * FROM Dimension.DimSentimiento ORDER BY Sentimiento_id;
SELECT TOP 5 * FROM Dimension.DimTiempo ORDER BY Tiempo_id;
GO

PRINT '--- Fact table counts and last load time ---';
SELECT 'FactOpiniones' AS TableName, COUNT(*) AS TotalRows, MAX(Fecha_carga) AS LastLoad FROM Fact.FactOpiniones;
SELECT 'FactTendencias' AS TableName, COUNT(*) AS TotalRows FROM Fact.FactTendencias;
GO

PRINT '--- Indexes on Fact and Dimension tables ---';
SELECT
    OBJECT_SCHEMA_NAME(i.object_id) AS SchemaName,
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    i.is_unique
FROM sys.indexes i
WHERE OBJECT_SCHEMA_NAME(i.object_id) IN ('Fact','Dimension')
ORDER BY TableName, IndexName;
GO

PRINT '--- Space used by key tables (sp_spaceused) ---';
EXEC sp_spaceused 'Fact.FactOpiniones';
EXEC sp_spaceused 'Dimension.DimProducto';
EXEC sp_spaceused 'Dimension.DimTiempo';
EXEC sp_spaceused 'Staging.StgOpiniones';
GO

PRINT '--- End of diagnostics ---';
GO
