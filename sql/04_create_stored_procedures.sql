




USE [DWOpinionClientes]
GO






CREATE OR ALTER PROCEDURE Fact.sp_CalcularTendencias
    @FechaInicio DATE = NULL,
    @FechaFin DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    

    IF @FechaInicio IS NULL SET @FechaInicio = DATEADD(DAY, -30, GETDATE())
    IF @FechaFin IS NULL SET @FechaFin = GETDATE()
    
    BEGIN TRY

        DECLARE @TiempoInicio INT = (SELECT MIN(Tiempo_id) FROM Dimension.DimTiempo WHERE Fecha >= @FechaInicio)
        DECLARE @TiempoFin INT = (SELECT MAX(Tiempo_id) FROM Dimension.DimTiempo WHERE Fecha <= @FechaFin)
        

        DELETE FROM Fact.FactTendencias
        WHERE Tiempo_id BETWEEN @TiempoInicio AND @TiempoFin
        

        INSERT INTO Fact.FactTendencias (
            Tendencia_id,
            Tiempo_id,
            Producto_id,
            Fuente_id,
            Total_opiniones,
            Promedio_satisfaccion,
            Porcentaje_positivas,
            Porcentaje_negativas,
            Porcentaje_neutras,
            Nps,
            Csat
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY t.Tiempo_id, p.Producto_id) AS Tendencia_id,
            t.Tiempo_id,
            p.Producto_id,
            NULL AS Fuente_id,
            COUNT(f.Opinion_id) AS Total_opiniones,
            AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS Promedio_satisfaccion,
            ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Porcentaje_positivas,
            ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Porcentaje_negativas,
            ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Neutral' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Porcentaje_neutras,
            ROUND((SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) - 
                   SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END)) * 100.0 / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Nps,
            ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Csat
        FROM Dimension.DimTiempo t
        CROSS JOIN Dimension.DimProducto p
        LEFT JOIN Fact.FactOpiniones f ON f.Tiempo_id = t.Tiempo_id AND f.Producto_id = p.Producto_id
        LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
        WHERE t.Tiempo_id BETWEEN @TiempoInicio AND @TiempoFin
        GROUP BY t.Tiempo_id, p.Producto_id
        HAVING COUNT(f.Opinion_id) > 0
        

        INSERT INTO Fact.FactTendencias (
            Tendencia_id,
            Tiempo_id,
            Producto_id,
            Fuente_id,
            Total_opiniones,
            Promedio_satisfaccion,
            Porcentaje_positivas,
            Porcentaje_negativas,
            Porcentaje_neutras,
            Nps,
            Csat
        )
        SELECT 
            ROW_NUMBER() OVER (ORDER BY t.Tiempo_id, fu.Fuente_id) + 
            (SELECT ISNULL(MAX(Tendencia_id), 0) FROM Fact.FactTendencias) AS Tendencia_id,
            t.Tiempo_id,
            NULL AS Producto_id,
            fu.Fuente_id,
            COUNT(f.Opinion_id) AS Total_opiniones,
            AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS Promedio_satisfaccion,
            ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Porcentaje_positivas,
            ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Porcentaje_negativas,
            ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Neutral' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Porcentaje_neutras,
            ROUND((SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) - 
                   SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END)) * 100.0 / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Nps,
            ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / 
                  NULLIF(COUNT(f.Opinion_id), 0), 2) AS Csat
        FROM Dimension.DimTiempo t
        CROSS JOIN Dimension.DimFuente fu
        LEFT JOIN Fact.FactOpiniones f ON f.Tiempo_id = t.Tiempo_id AND f.Fuente_id = fu.Fuente_id
        LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
        WHERE t.Tiempo_id BETWEEN @TiempoInicio AND @TiempoFin
        GROUP BY t.Tiempo_id, fu.Fuente_id
        HAVING COUNT(f.Opinion_id) > 0
        
        PRINT CONCAT('Tendencias calculadas para rango: ', @FechaInicio, ' a ', @FechaFin)
        RETURN 0
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMsg NVARCHAR(MAX) = ERROR_MESSAGE()
        RAISERROR('Error al calcular tendencias: %s', 16, 1, @ErrorMsg)
        RETURN -1
    END CATCH
END
GO






CREATE OR ALTER PROCEDURE Fact.sp_ObtenerKPIs
    @FechaInicio DATE = NULL,
    @FechaFin DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @FechaInicio IS NULL SET @FechaInicio = DATEADD(MONTH, -1, GETDATE())
    IF @FechaFin IS NULL SET @FechaFin = GETDATE()
    

    SELECT 
        COUNT(f.Opinion_id) AS TotalOpiniones,
        AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS SatisfaccionPromedio,
        SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) AS OpinionesPositivas,
        SUM(CASE WHEN s.Clasificacion = 'Neutral' THEN 1 ELSE 0 END) AS OpinionesNeutras,
        SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) AS OpinionesNegativas,
        ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS PorcentajePositivas,
        ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS PorcentajeNegativas,
        ROUND((SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) - 
               SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END)) * 100.0 / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS NPS
    FROM Fact.FactOpiniones f
    INNER JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
    LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
    WHERE t.Fecha BETWEEN @FechaInicio AND @FechaFin
END
GO






CREATE OR ALTER PROCEDURE Fact.sp_ObtenerTopProductos
    @Top INT = 10,
    @FechaInicio DATE = NULL,
    @FechaFin DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @FechaInicio IS NULL SET @FechaInicio = DATEADD(MONTH, -3, GETDATE())
    IF @FechaFin IS NULL SET @FechaFin = GETDATE()
    
    SELECT TOP (@Top)
        p.Producto_id,
        p.Nombre_producto,
        p.Categoria,
        COUNT(f.Opinion_id) AS TotalOpiniones,
        AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS SatisfaccionPromedio,
        ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS PorcentajePositivas,
        ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS PorcentajeNegativas,
        ROUND((SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) - 
               SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END)) * 100.0 / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS NPS
    FROM Fact.FactOpiniones f
    INNER JOIN Dimension.DimProducto p ON f.Producto_id = p.Producto_id
    INNER JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
    LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
    WHERE t.Fecha BETWEEN @FechaInicio AND @FechaFin
      AND p.activo = 1
    GROUP BY p.Producto_id, p.Nombre_producto, p.Categoria
    ORDER BY SatisfaccionPromedio DESC
END
GO






CREATE OR ALTER PROCEDURE Fact.sp_ObtenerTendenciasMes
    @Meses INT = 12
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @FechaInicio DATE = DATEADD(MONTH, -@Meses, GETDATE())
    DECLARE @FechaFin DATE = GETDATE()
    
    SELECT 
        CONCAT(MONTH(t.Fecha), '/', YEAR(t.Fecha)) AS MesAÃ±o,
        YEAR(t.Fecha) AS AÃ±o,
        MONTH(t.Fecha) AS Mes,
        COUNT(f.Opinion_id) AS TotalOpiniones,
        AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS SatisfaccionPromedio,
        ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS PorcentajePositivas,
        ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS PorcentajeNegativas,
        ROUND((SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) - 
               SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END)) * 100.0 / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS NPS
    FROM Fact.FactOpiniones f
    INNER JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
    LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
    WHERE t.Fecha BETWEEN @FechaInicio AND @FechaFin
    GROUP BY YEAR(t.Fecha), MONTH(t.Fecha)
    ORDER BY AÃ±o DESC, Mes DESC
END
GO






CREATE OR ALTER PROCEDURE Fact.sp_ObtenerOpinionsPorCanal
    @FechaInicio DATE = NULL,
    @FechaFin DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @FechaInicio IS NULL SET @FechaInicio = DATEADD(MONTH, -3, GETDATE())
    IF @FechaFin IS NULL SET @FechaFin = GETDATE()
    
    SELECT 
        fu.Fuente_id,
        fu.Tipo_fuente,
        fu.Canal,
        COUNT(f.Opinion_id) AS TotalOpiniones,
        AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS SatisfaccionPromedio,
        ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS PorcentajePositivas,
        ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS PorcentajeNegativas,
        ROUND((SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) - 
               SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END)) * 100.0 / 
              NULLIF(COUNT(f.Opinion_id), 0), 2) AS NPS
    FROM Fact.FactOpiniones f
    INNER JOIN Dimension.DimFuente fu ON f.Fuente_id = fu.Fuente_id
    INNER JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
    LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
    WHERE t.Fecha BETWEEN @FechaInicio AND @FechaFin
    GROUP BY fu.Fuente_id, fu.Tipo_fuente, fu.Canal
    ORDER BY TotalOpiniones DESC
END
GO






CREATE OR ALTER PROCEDURE Fact.sp_ObtenerOpinionesDetalladas
    @ProductoId INT = NULL,
    @ClienteId INT = NULL,
    @Sentimiento NVARCHAR(20) = NULL,
    @FechaInicio DATE = NULL,
    @FechaFin DATE = NULL,
    @PageNumber INT = 1,
    @PageSize INT = 50
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @FechaInicio IS NULL SET @FechaInicio = DATEADD(MONTH, -6, GETDATE())
    IF @FechaFin IS NULL SET @FechaFin = GETDATE()
    
    DECLARE @Offset INT = (@PageNumber - 1) * @PageSize
    
    SELECT 
        f.Opinion_id,
        t.Fecha,
        p.Nombre_producto,
        p.Categoria,
        cl.Nombre_Cliente,
        cl.Email,
        fu.Tipo_fuente,
        fu.Canal,
        s.Clasificacion,
        f.Puntaje_Satisfaccion,
        f.Rating,
        f.Comentario,
        f.Longitud_Comentario,
        f.Score_sentimiento
    FROM Fact.FactOpiniones f
    INNER JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
    INNER JOIN Dimension.DimProducto p ON f.Producto_id = p.Producto_id
    INNER JOIN Dimension.DimCliente cl ON f.Cliente_id = cl.Cliente_id
    INNER JOIN Dimension.DimFuente fu ON f.Fuente_id = fu.Fuente_id
    LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
    WHERE t.Fecha BETWEEN @FechaInicio AND @FechaFin
      AND (@ProductoId IS NULL OR f.Producto_id = @ProductoId)
      AND (@ClienteId IS NULL OR f.Cliente_id = @ClienteId)
      AND (@Sentimiento IS NULL OR s.Clasificacion = @Sentimiento)
    ORDER BY f.Opinion_id DESC
    OFFSET @Offset ROWS
    FETCH NEXT @PageSize ROWS ONLY
END
GO

PRINT 'âœ“ Procedimientos almacenados creados exitosamente'
GO
