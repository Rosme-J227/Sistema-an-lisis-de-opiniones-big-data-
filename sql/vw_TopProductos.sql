USE DWOpinionClientes;
GO

IF OBJECT_ID('vw_TopProductos','V') IS NOT NULL
    DROP VIEW vw_TopProductos;
GO

CREATE VIEW vw_TopProductos
AS
SELECT TOP 100
    p.Producto_id,
    p.Nombre_producto AS ProductoNombre,
    p.Categoria,
    COUNT(f.Opinion_id) AS TotalOpiniones,
    AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS PromedioSatisfaccion,
    CASE WHEN COUNT(f.Opinion_id) = 0 THEN 0
         ELSE ROUND(100.0 * SUM(CASE WHEN s.Clasificacion = 'Positive' THEN 1 ELSE 0 END) / COUNT(f.Opinion_id), 2)
    END AS PorcentajePositivas
FROM Fact.FactOpiniones f
LEFT JOIN Dimension.DimProducto p ON f.Producto_id = p.Producto_id
LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
GROUP BY p.Producto_id, p.Nombre_producto, p.Categoria
ORDER BY TotalOpiniones DESC;
GO
