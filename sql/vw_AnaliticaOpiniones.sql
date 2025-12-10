USE DWOpinionClientes;
GO

IF OBJECT_ID('vw_AnaliticaOpiniones','V') IS NOT NULL
    DROP VIEW vw_AnaliticaOpiniones;
GO

CREATE VIEW vw_AnaliticaOpiniones
AS
SELECT
    f.Opinion_id,
    f.Fecha_carga,
    t.Fecha AS Fecha,
    t.Tiempo_id,
    p.Producto_id,
    p.Nombre_producto AS ProductoNombre,
    p.Categoria,
    cl.Cliente_id,
    cl.Nombre_Cliente AS ClienteNombre,
    cl.Email,
    fu.Fuente_id,
    fu.Tipo_fuente,
    fu.Canal,
    s.Sentimiento_id,
    s.Clasificacion AS Sentimiento,
    f.Puntaje_Satisfaccion,
    f.Rating,
    f.Comentario,
    f.Longitud_Comentario,
    f.Score_sentimiento
FROM Fact.FactOpiniones f
LEFT JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
LEFT JOIN Dimension.DimProducto p ON f.Producto_id = p.Producto_id
LEFT JOIN Dimension.DimCliente cl ON f.Cliente_id = cl.Cliente_id
LEFT JOIN Dimension.DimFuente fu ON f.Fuente_id = fu.Fuente_id
LEFT JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id;
GO
