




USE [DWOpinionClientes]
GO






PRINT 'Insertando datos en DimSentimiento...'
GO


DELETE FROM Dimension.DimSentimiento
GO


INSERT INTO Dimension.DimSentimiento (Sentimiento_id, Clasificacion, Puntuacion_min, Puntuacion_max, Color)
VALUES 
    (1, 'Positive', 70, 100, 'Green'),
    (2, 'Neutral', 40, 69, 'Gray'),
    (3, 'Negative', 0, 39, 'Red')
GO

PRINT 'DimSentimiento seeded: 3 registros insertados'
GO






PRINT 'Insertando datos en DimFuente...'
GO

DELETE FROM Dimension.DimFuente
GO

INSERT INTO Dimension.DimFuente (Fuente_id, Tipo_fuente, Plataforma, Canal)
VALUES 
    (1, 'CSV', 'Encuestas Internas', 'Encuesta'),
    (2, 'DATABASE', 'Sitio Web', 'Web'),
    (3, 'API', 'Twitter', 'Redes Sociales'),
    (4, 'API', 'Instagram', 'Redes Sociales'),
    (5, 'API', 'Facebook', 'Redes Sociales'),
    (6, 'DATABASE', 'Google Reviews', 'Web'),
    (7, 'DATABASE', 'Tripadvisor', 'Web')
GO

PRINT 'DimFuente seeded: 7 registros insertados'
GO






PRINT 'Insertando datos en DimTiempo...'
GO


DELETE FROM Dimension.DimTiempo
GO


DECLARE @FechaInicio DATE = '2022-01-01'
DECLARE @FechaFin DATE = '2024-12-31'
DECLARE @FechaActual DATE = @FechaInicio
DECLARE @Tiempo_id INT = 0

WHILE @FechaActual <= @FechaFin
BEGIN
    SET @Tiempo_id = @Tiempo_id + 1
    
    INSERT INTO Dimension.DimTiempo (
        Tiempo_id,
        Fecha,
        Dia,
        Mes,
        AÃ±o,
        Trimestre,
        Nombre_Mes,
        Nombre_dia,
        Mes_aÃ±o,
        Es_Fin_Semana
    )
    VALUES (
        @Tiempo_id,
        @FechaActual,
        DAY(@FechaActual),
        MONTH(@FechaActual),
        YEAR(@FechaActual),
        ((MONTH(@FechaActual) - 1) / 3) + 1,
        FORMAT(@FechaActual, 'MMMM', 'es-ES'),
        FORMAT(@FechaActual, 'dddd', 'es-ES'),
        CONCAT(MONTH(@FechaActual), '/', YEAR(@FechaActual)),
        CASE WHEN DATEPART(WEEKDAY, @FechaActual) IN (1, 7) THEN 1 ELSE 0 END
    )
    
    SET @FechaActual = DATEADD(DAY, 1, @FechaActual)
END

PRINT CONCAT('DimTiempo seeded: ', @Tiempo_id, ' dÃ­as insertados (2022-2024)')
GO





PRINT 'Insertando datos iniciales en DimProducto...'
GO


INSERT INTO Dimension.DimProducto (Producto_id, Id_Producto_Origen, Nombre_producto, Categoria, Fecha_alta, activo)
VALUES 
    (1, 'P001', 'Laptop XPS 13', 'ElectrÃ³nica', '2023-01-15', 1),
    (2, 'P002', 'Monitor 4K 32"', 'ElectrÃ³nica', '2023-02-10', 1),
    (3, 'P003', 'Teclado MecÃ¡nico RGB', 'Accesorios', '2023-03-05', 1),
    (4, 'P004', 'RatÃ³n InalÃ¡mbrico', 'Accesorios', '2023-04-12', 1),
    (5, 'P005', 'Webcam 1080p', 'Accesorios', '2023-05-20', 1),
    (6, 'P006', 'Auriculares Bluetooth', 'Accesorios', '2023-06-08', 1),
    (7, 'P007', 'SSD 1TB NVMe', 'Almacenamiento', '2023-07-14', 1),
    (8, 'P008', 'Hub USB-C', 'Accesorios', '2023-08-22', 1)
GO

PRINT 'DimProducto seeded: 8 productos de ejemplo insertados'
GO





PRINT 'Insertando datos iniciales en DimCliente...'
GO

INSERT INTO Dimension.DimCliente (Cliente_id, Id_Cliente_origen, Nombre_Cliente, Email, Segmento, Pais, Rango_Edad)
VALUES 
    (1, '1001', 'Juan GarcÃ­a', 'juan@example.com', 'Premium', 'EspaÃ±a', '25-35'),
    (2, '1002', 'MarÃ­a LÃ³pez', 'maria@example.com', 'Standard', 'EspaÃ±a', '35-45'),
    (3, '1003', 'Carlos RodrÃ­guez', 'carlos@example.com', 'Premium', 'MÃ©xico', '45-55'),
    (4, '1004', 'Ana MartÃ­nez', 'ana@example.com', 'Standard', 'Colombia', '18-25'),
    (5, '1005', 'Pedro SÃ¡nchez', 'pedro@example.com', 'Basic', 'Argentina', '55+'),
    (6, '1006', 'Laura FernÃ¡ndez', 'laura@example.com', 'Premium', 'Chile', '25-35'),
    (7, '1007', 'Diego Morales', 'diego@example.com', 'Standard', 'PerÃº', '35-45'),
    (8, '1008', 'Sofia GonzÃ¡lez', 'sofia@example.com', 'Basic', 'EspaÃ±a', '18-25')
GO

PRINT 'DimCliente seeded: 8 clientes de ejemplo insertados'
GO





PRINT ''
PRINT '=== VERIFICACIÃ“N DE DATOS SEEDED ==='
GO

SELECT 'DimSentimiento' AS Tabla, COUNT(*) AS Registros FROM Dimension.DimSentimiento
UNION ALL
SELECT 'DimFuente', COUNT(*) FROM Dimension.DimFuente
UNION ALL
SELECT 'DimTiempo', COUNT(*) FROM Dimension.DimTiempo
UNION ALL
SELECT 'DimProducto', COUNT(*) FROM Dimension.DimProducto
UNION ALL
SELECT 'DimCliente', COUNT(*) FROM Dimension.DimCliente
GO

PRINT ''
PRINT 'Primeras filas de DimFuente:'
SELECT * FROM Dimension.DimFuente ORDER BY Fuente_id
GO

PRINT ''
PRINT 'Primeras filas de DimProducto:'
SELECT TOP 5 * FROM Dimension.DimProducto ORDER BY Producto_id
GO

PRINT ''
PRINT 'Primeras filas de DimCliente:'
SELECT TOP 5 * FROM Dimension.DimCliente ORDER BY Cliente_id
GO

PRINT ''
PRINT 'âœ“ Script de Seed Data completado exitosamente'
GO
