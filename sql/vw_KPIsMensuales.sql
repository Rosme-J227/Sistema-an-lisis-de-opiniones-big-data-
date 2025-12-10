USE DWOpinionClientes;
GO

IF OBJECT_ID('vw_KPIsMensuales','V') IS NOT NULL
    DROP VIEW vw_KPIsMensuales;
GO

CREATE VIEW vw_KPIsMensuales
AS
SELECT
    YEAR(t.Fecha) AS Ano,
    MONTH(t.Fecha) AS Mes,
     CONCAT(MONTH(t.Fecha),'/',YEAR(t.Fecha)) AS MesAno,
    p.Producto_id,
    p.Nombre_producto AS ProductoNombre,
    COUNT(f.Opinion_id) AS TotalOpiniones,
    AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS PromedioSatisfaccion,
    SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) AS Positivas,
    SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) AS Negativas,
    SUM(CASE WHEN s.Clasificacion = 'Neutral' THEN 1 ELSE 0 END) AS Neutras,
    CASE WHEN COUNT(f.Opinion_id) = 0 THEN 0
         ELSE ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / COUNT(f.Opinion_id), 2)
    END AS PorcentajePositivas,
    CASE WHEN COUNT(f.Opinion_id) = 0 THEN 0
         ELSE ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END) / COUNT(f.Opinion_id), 2)
    END AS PorcentajeNegativas,
    CASE WHEN COUNT(f.Opinion_id) = 0 THEN 0
         ELSE ROUND((SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) - SUM(CASE WHEN s.Clasificacion = 'Negative' THEN 1 ELSE 0 END)) * 100.0 / COUNT(f.Opinion_id), 2)
    END AS NPS
FROM Fact.FactOpiniones f
INNER JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
LEFT JOIN Dimension.DimProducto p ON f.Producto_id = p.Producto_id
LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
GROUP BY YEAR(t.Fecha), MONTH(t.Fecha), CONCAT(MONTH(t.Fecha),'/',YEAR(t.Fecha)), p.Producto_id, p.Nombre_producto;
GO
