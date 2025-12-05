SET NOCOUNT ON;
USE DWOpinionClientes;
GO
BEGIN TRY
    BEGIN TRANSACTION;

    PRINT 'Upserting DimCliente from SYN rows...';

    INSERT INTO Dimension.DimCliente (Id_Cliente_origen, Nombre_Cliente, Email, Segmento, Pais, Rango_Edad)
    SELECT DISTINCT
        CASE WHEN s.ClienteId IS NOT NULL THEN CAST(s.ClienteId AS NVARCHAR(50)) ELSE NULL END AS Id_Cliente_origen,
        s.NombreCliente,
        s.Email,
        NULL, NULL, NULL
    FROM Staging.StgOpiniones s
    WHERE s.EstadoCarga = 'NEW' AND s.IdOrigen LIKE 'SYN%'
      AND (
          (s.ClienteId IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Dimension.DimCliente c WHERE c.Id_Cliente_origen = CAST(s.ClienteId AS NVARCHAR(50))))
       OR (s.ClienteId IS NULL AND s.Email IS NOT NULL AND NOT EXISTS (SELECT 1 FROM Dimension.DimCliente c WHERE c.Email = s.Email))
      );

    PRINT 'Upserting DimProducto from SYN rows...';

    INSERT INTO Dimension.DimProducto (Id_Producto_Origen, Nombre_producto, Categoria, Fecha_alta, activo)
    SELECT DISTINCT
        s.CodigoProducto,
        s.CodigoProducto,
        'Desconocida',
        GETDATE(),
        1
    FROM Staging.StgOpiniones s
    WHERE s.EstadoCarga = 'NEW' AND s.IdOrigen LIKE 'SYN%'
      AND s.CodigoProducto IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM Dimension.DimProducto p WHERE p.Id_Producto_Origen = s.CodigoProducto);

    PRINT 'Upserting DimFuente from SYN rows...';

    INSERT INTO Dimension.DimFuente (Fuente_id, Tipo_fuente, Plataforma, Canal)
    SELECT DISTINCT
        (SELECT ISNULL(MAX(Fuente_id),0) + ROW_NUMBER() OVER (ORDER BY t.Tipo) + (SELECT ISNULL(MAX(Fuente_id),0) FROM Dimension.DimFuente) - 1) AS NewId,
        t.Tipo, NULL, NULL
    FROM (
        SELECT DISTINCT s.Fuente AS Tipo FROM Staging.StgOpiniones s WHERE s.EstadoCarga='NEW' AND s.IdOrigen LIKE 'SYN%'
    ) t
    WHERE t.Tipo IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM Dimension.DimFuente f WHERE f.Tipo_fuente = t.Tipo);

    PRINT 'Upserting DimTiempo from SYN rows...';

    INSERT INTO Dimension.DimTiempo (Fecha, Dia, Mes, [AÃ±o], Trimestre, Nombre_Mes, Nombre_dia, Mes_aÃ±o, Es_Fin_Semana)
    SELECT DISTINCT
        CONVERT(date, s.FechaOrigen) AS Fecha,
        DATEPART(day, s.FechaOrigen) AS Dia,
        DATEPART(month, s.FechaOrigen) AS Mes,
      DATEPART(year, s.FechaOrigen) AS [AÃ±o],
        ((DATEPART(month, s.FechaOrigen)-1)/3)+1 AS Trimestre,
        DATENAME(month, s.FechaOrigen) AS Nombre_Mes,
        DATENAME(weekday, s.FechaOrigen) AS Nombre_dia,
        RIGHT('0' + CAST(DATEPART(month, s.FechaOrigen) AS VARCHAR(2)),2) + '-' + CAST(DATEPART(year, s.FechaOrigen) AS VARCHAR(4)) AS Mes_aÃ±o,
        CASE WHEN DATEPART(weekday, s.FechaOrigen) IN (1,7) THEN 1 ELSE 0 END AS Es_Fin_Semana
    FROM Staging.StgOpiniones s
    WHERE s.EstadoCarga = 'NEW' AND s.IdOrigen LIKE 'SYN%'
      AND s.FechaOrigen IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM Dimension.DimTiempo t WHERE t.Fecha = CONVERT(date, s.FechaOrigen));

    COMMIT TRANSACTION;
    PRINT 'Upsert transaction committed.';
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION;
    PRINT 'ERROR: ' + ERROR_MESSAGE();
    THROW;
END CATCH
GO

SELECT 'DimCliente total' AS Item, COUNT(*) AS Total FROM Dimension.DimCliente;
SELECT 'DimProducto total' AS Item, COUNT(*) AS Total FROM Dimension.DimProducto;
SELECT 'DimFuente total' AS Item, COUNT(*) AS Total FROM Dimension.DimFuente;
SELECT 'DimTiempo total' AS Item, COUNT(*) AS Total FROM Dimension.DimTiempo;
GO
