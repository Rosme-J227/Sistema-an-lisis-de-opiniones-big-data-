SET NOCOUNT ON;
USE SourceOpiniones;
GO
BEGIN TRY
    SET XACT_ABORT ON;
    BEGIN TRANSACTION;

    DECLARE @total INT = 2500;


    IF OBJECT_ID('tempdb..#clientes') IS NOT NULL DROP TABLE #clientes;
    IF OBJECT_ID('tempdb..#productos') IS NOT NULL DROP TABLE #productos;

    SELECT ROW_NUMBER() OVER (ORDER BY ClienteId) AS rn, ClienteId INTO #clientes FROM Clientes;
    SELECT ROW_NUMBER() OVER (ORDER BY ProductoId) AS rn, ProductoId INTO #productos FROM Productos;

    DECLARE @cntClientes INT = (SELECT COUNT(*) FROM #clientes);
    DECLARE @cntProductos INT = (SELECT COUNT(*) FROM #productos);

    IF @cntClientes = 0 OR @cntProductos = 0
    BEGIN
        RAISERROR('No hay Clientes o Productos en SourceOpiniones; primero seedearlos.',16,1);
    END

    ;WITH nums AS (
        SELECT 1 AS n
        UNION ALL
        SELECT n+1 FROM nums WHERE n+1 <= @total
    )
    INSERT INTO Reviews (IdOrigen, ClienteId, ProductoId, Fuente, Fecha, Comentario, Puntaje, Rating)
    SELECT
        'SYN' + RIGHT('00000' + CAST(n AS VARCHAR(6)),6) AS IdOrigen,
        c.ClienteId,
        p.ProductoId,
        CASE (n % 3) WHEN 0 THEN 'SYN' WHEN 1 THEN 'API' ELSE 'CSV' END AS Fuente,
        DATEADD(day, - (n % 365), GETDATE()) AS Fecha,
        'Synthetic review #' + CAST(n AS VARCHAR(10)) AS Comentario,
        ((n - 1) % 5) + 1 AS Puntaje,
        ((n - 1) % 5) + 1 AS Rating
    FROM nums
    CROSS APPLY (SELECT ClienteId FROM #clientes WHERE rn = ((n-1) % @cntClientes) + 1) c
    CROSS APPLY (SELECT ProductoId FROM #productos WHERE rn = ((n-1) % @cntProductos) + 1) p
    OPTION (MAXRECURSION 0);

    COMMIT TRANSACTION;
    PRINT 'Inserted ' + CAST(@total AS VARCHAR(10)) + ' synthetic reviews.';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    DECLARE @err NVARCHAR(4000) = ERROR_MESSAGE();
    PRINT 'ERROR: ' + @err;
    THROW;
END CATCH
GO

SELECT COUNT(*) AS TotalSynthetic FROM Reviews WHERE IdOrigen LIKE 'SYN%';
GO
