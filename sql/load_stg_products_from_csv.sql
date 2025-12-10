USE DWOpinionClientes;
GO

IF OBJECT_ID('tempdb..#tmpProducts') IS NOT NULL DROP TABLE #tmpProducts;

CREATE TABLE #tmpProducts (
    IdProducto NVARCHAR(100),
    Nombre NVARCHAR(200),
    Categoria NVARCHAR(100)
);

BULK INSERT #tmpProducts
FROM 'C:\Users\marielys j\Downloads\CustomerOpinionsETL_Project\csv\products.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001',
    TABLOCK
);

-- Insertar solo filas nuevas evitando duplicados por CodigoProducto
INSERT INTO Staging.StgProductos (CodigoProducto, NombreProducto, Categoria)
SELECT DISTINCT
    IdProducto,
    Nombre,
    Categoria
FROM #tmpProducts t
WHERE ISNULL(IdProducto, '') <> ''
  AND NOT EXISTS (SELECT 1 FROM Staging.StgProductos p WHERE p.CodigoProducto = t.IdProducto);

SELECT 'Productos importados' AS Info, COUNT(*) AS Filas FROM Staging.StgProductos;
GO
