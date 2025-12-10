USE DWOpinionClientes;
GO

IF OBJECT_ID('tempdb..#tmpClients') IS NOT NULL DROP TABLE #tmpClients;

CREATE TABLE #tmpClients (
    IdCliente NVARCHAR(100),
    Nombre NVARCHAR(200),
    Email NVARCHAR(200)
);

BULK INSERT #tmpClients
FROM 'C:\Users\marielys j\Downloads\CustomerOpinionsETL_Project\csv\clients.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CODEPAGE = '65001',
    TABLOCK
);

-- Insertar solo filas nuevas evitando duplicados por Email
INSERT INTO Staging.StgClientes (Email, NombreCliente)
SELECT DISTINCT
    Email,
    Nombre
FROM #tmpClients c
WHERE ISNULL(Email, '') <> ''
  AND NOT EXISTS (SELECT 1 FROM Staging.StgClientes s WHERE s.Email = c.Email);

SELECT 'Clientes importados' AS Info, COUNT(*) AS Filas FROM Staging.StgClientes;
GO
