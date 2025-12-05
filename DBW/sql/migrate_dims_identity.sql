-- Migration script: convert DimProducto, DimSentimiento, DimTiempo to use IDENTITY
-- Saves mapping OldId -> NewId and updates Fact tables accordingly.
-- IMPORTANT: Make a full backup before running. Test on a copy first.

USE [DWOpinionClientes];
GO

-- Temp mapping tables
IF OBJECT_ID('tempdb..#ProdMap') IS NOT NULL DROP TABLE #ProdMap;
CREATE TABLE #ProdMap (OldProductoId INT, NewProductoId INT);

IF OBJECT_ID('tempdb..#SentMap') IS NOT NULL DROP TABLE #SentMap;
CREATE TABLE #SentMap (OldSentId INT, NewSentId INT);

IF OBJECT_ID('tempdb..#TimeMap') IS NOT NULL DROP TABLE #TimeMap;
CREATE TABLE #TimeMap (OldTimeId INT, NewTimeId INT);

BEGIN TRY
    BEGIN TRANSACTION;

    -------------------------------------------------------------
    -- DimProducto -> new table with IDENTITY
    -------------------------------------------------------------
    IF OBJECT_ID('Dimension.DimProducto_new') IS NOT NULL DROP TABLE Dimension.DimProducto_new;
    CREATE TABLE Dimension.DimProducto_new (
        Producto_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Id_Producto_Origen NVARCHAR(50) NULL,
        Nombre_producto NVARCHAR(100) NOT NULL,
        Categoria NVARCHAR(100) NULL,
        Fecha_alta DATE NULL,
        activo BIT NULL
    ) ON [PRIMARY];

    INSERT INTO Dimension.DimProducto_new (Id_Producto_Origen, Nombre_producto, Categoria, Fecha_alta, activo)
    OUTPUT src.Producto_id, inserted.Producto_id INTO #ProdMap(OldProductoId, NewProductoId)
    SELECT TRY_CAST(Id_Producto_Origen AS NVARCHAR(50)), Nombre_producto, Categoria, Fecha_alta, activo
    FROM Dimension.DimProducto AS src
    ORDER BY src.Producto_id;

    -------------------------------------------------------------
    -- DimSentimiento -> new table with IDENTITY
    -------------------------------------------------------------
    IF OBJECT_ID('Dimension.DimSentimiento_new') IS NOT NULL DROP TABLE Dimension.DimSentimiento_new;
    CREATE TABLE Dimension.DimSentimiento_new (
        Sentimiento_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Clasificacion NVARCHAR(20) NOT NULL,
        Puntuacion_min INT NOT NULL,
        Puntuacion_max INT NOT NULL,
        Color NVARCHAR(20) NULL
    ) ON [PRIMARY];

    INSERT INTO Dimension.DimSentimiento_new (Clasificacion, Puntuacion_min, Puntuacion_max, Color)
    OUTPUT src.Sentimiento_id, inserted.Sentimiento_id INTO #SentMap(OldSentId, NewSentId)
    SELECT Clasificacion, Puntuacion_min, Puntuacion_max, Color
    FROM Dimension.DimSentimiento AS src
    ORDER BY src.Sentimiento_id;

    -------------------------------------------------------------
    -- DimTiempo -> new table with IDENTITY
    -------------------------------------------------------------
    IF OBJECT_ID('Dimension.DimTiempo_new') IS NOT NULL DROP TABLE Dimension.DimTiempo_new;
    CREATE TABLE Dimension.DimTiempo_new (
        Tiempo_id INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
        Fecha DATE NOT NULL,
        Dia INT NOT NULL,
        Mes INT NOT NULL,
        [Año] INT NOT NULL,
        Trimestre INT NOT NULL,
        Nombre_Mes NVARCHAR(20) NOT NULL,
        Nombre_dia NVARCHAR(20) NOT NULL,
        Mes_año NVARCHAR(20) NOT NULL,
        Es_Fin_Semana BIT NULL
    ) ON [PRIMARY];

    INSERT INTO Dimension.DimTiempo_new (Fecha, Dia, Mes, [Año], Trimestre, Nombre_Mes, Nombre_dia, Mes_año, Es_Fin_Semana)
    OUTPUT src.Tiempo_id, inserted.Tiempo_id INTO #TimeMap(OldTimeId, NewTimeId)
    SELECT Fecha, Dia, Mes, [Año], Trimestre, Nombre_Mes, Nombre_dia, Mes_año, Es_Fin_Semana
    FROM Dimension.DimTiempo AS src
    ORDER BY src.Tiempo_id;

    -------------------------------------------------------------
    -- Drop known foreign keys that reference the old dimension tables
    -------------------------------------------------------------
    IF OBJECT_ID('Fact.FactOpiniones','U') IS NOT NULL
    BEGIN
        IF OBJECT_ID('FK_FactOpiniones_DimProducto','F') IS NOT NULL
            ALTER TABLE Fact.FactOpiniones DROP CONSTRAINT FK_FactOpiniones_DimProducto;
        IF OBJECT_ID('FK_FactOpiniones_DimSentimiento','F') IS NOT NULL
            ALTER TABLE Fact.FactOpiniones DROP CONSTRAINT FK_FactOpiniones_DimSentimiento;
        IF OBJECT_ID('FK_FactOpiniones_DimTiempo','F') IS NOT NULL
            ALTER TABLE Fact.FactOpiniones DROP CONSTRAINT FK_FactOpiniones_DimTiempo;
    END

    IF OBJECT_ID('Fact.FactTendencias','U') IS NOT NULL
    BEGIN
        IF OBJECT_ID('FK_FactTendencias_DimProducto','F') IS NOT NULL
            ALTER TABLE Fact.FactTendencias DROP CONSTRAINT FK_FactTendencias_DimProducto;
        IF OBJECT_ID('FK_FactTendencias_DimTiempo','F') IS NOT NULL
            ALTER TABLE Fact.FactTendencias DROP CONSTRAINT FK_FactTendencias_DimTiempo;
    END

    -------------------------------------------------------------
    -- Update Fact tables using mapping
    -------------------------------------------------------------
    -- Update FactOpiniones.Producto_id
    UPDATE f
    SET Producto_id = m.NewProductoId
    FROM Fact.FactOpiniones f
    JOIN #ProdMap m ON f.Producto_id = m.OldProductoId;

    -- Update FactOpiniones.Sentimiento_id
    UPDATE f
    SET Sentimiento_id = m.NewSentId
    FROM Fact.FactOpiniones f
    JOIN #SentMap m ON f.Sentimiento_id = m.OldSentId;

    -- Update FactOpiniones.Tiempo_id
    UPDATE f
    SET Tiempo_id = m.NewTimeId
    FROM Fact.FactOpiniones f
    JOIN #TimeMap m ON f.Tiempo_id = m.OldTimeId;

    -- Update FactTendencias.Producto_id (may be NULL)
    UPDATE f
    SET Producto_id = m.NewProductoId
    FROM Fact.FactTendencias f
    JOIN #ProdMap m ON f.Producto_id = m.OldProductoId;

    -- Update FactTendencias.Tiempo_id
    UPDATE f
    SET Tiempo_id = m.NewTimeId
    FROM Fact.FactTendencias f
    JOIN #TimeMap m ON f.Tiempo_id = m.OldTimeId;

    -------------------------------------------------------------
    -- Drop old dimension tables and rename new ones
    -------------------------------------------------------------
    DROP TABLE Dimension.DimProducto;
    EXEC sp_rename 'Dimension.DimProducto_new', 'DimProducto';

    DROP TABLE Dimension.DimSentimiento;
    EXEC sp_rename 'Dimension.DimSentimiento_new', 'DimSentimiento';

    DROP TABLE Dimension.DimTiempo;
    EXEC sp_rename 'Dimension.DimTiempo_new', 'DimTiempo';

    -------------------------------------------------------------
    -- Recreate foreign keys for facts (names same as original)
    -------------------------------------------------------------
    ALTER TABLE Fact.FactOpiniones
        ADD CONSTRAINT FK_FactOpiniones_DimProducto FOREIGN KEY (Producto_id) REFERENCES Dimension.DimProducto (Producto_id);

    ALTER TABLE Fact.FactOpiniones
        ADD CONSTRAINT FK_FactOpiniones_DimSentimiento FOREIGN KEY (Sentimiento_id) REFERENCES Dimension.DimSentimiento (Sentimiento_id);

    ALTER TABLE Fact.FactOpiniones
        ADD CONSTRAINT FK_FactOpiniones_DimTiempo FOREIGN KEY (Tiempo_id) REFERENCES Dimension.DimTiempo (Tiempo_id);

    ALTER TABLE Fact.FactTendencias
        ADD CONSTRAINT FK_FactTendencias_DimProducto FOREIGN KEY (Producto_id) REFERENCES Dimension.DimProducto (Producto_id);

    ALTER TABLE Fact.FactTendencias
        ADD CONSTRAINT FK_FactTendencias_DimTiempo FOREIGN KEY (Tiempo_id) REFERENCES Dimension.DimTiempo (Tiempo_id);

    COMMIT TRANSACTION;
    PRINT 'Migration completed successfully.';
END TRY
BEGIN CATCH
    DECLARE @errMsg NVARCHAR(4000) = ERROR_MESSAGE();
    ROLLBACK TRANSACTION;
    PRINT 'Migration failed. Transaction rolled back. Error: ' + @errMsg;
    THROW;
END CATCH;

GO

-- Cleanup temp mappings (they are in tempdb and auto-dropped at session end)
