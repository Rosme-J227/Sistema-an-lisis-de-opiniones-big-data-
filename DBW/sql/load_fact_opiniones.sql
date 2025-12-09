USE DWOpinionClientes;
GO

/*
Procedure: Fact.sp_LoadFactOpiniones
Purpose: Load rows from Staging.StgOpiniones into Fact.FactOpiniones.
This is a best-effort bulk loader that maps dimensions by business keys when possible.
It does NOT attempt complex NLP sentiment scoring — it uses simple rules over Puntaje to pick Sentimiento.
It marks staging rows as PROCESSED when successfully inserted.
*/
CREATE OR ALTER PROCEDURE Fact.sp_LoadFactOpiniones
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @start DATETIME = GETDATE();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Temporary table with staging rows to process
        IF OBJECT_ID('tempdb..#ToProcess') IS NOT NULL DROP TABLE #ToProcess;
        CREATE TABLE #ToProcess (
            StgId BIGINT,
            Fuente NVARCHAR(50),
            IdOrigen NVARCHAR(100),
            ClienteId INT NULL,
            NombreCliente NVARCHAR(200),
            Email NVARCHAR(200),
            CodigoProducto NVARCHAR(100),
            Comentario NVARCHAR(MAX),
            Puntaje INT NULL,
            Rating INT NULL,
            FechaOrigen DATETIME
        );

        INSERT INTO #ToProcess (StgId, Fuente, IdOrigen, ClienteId, NombreCliente, Email, CodigoProducto, Comentario, Puntaje, Rating, FechaOrigen)
        SELECT StgId, Fuente, IdOrigen, ClienteId, NombreCliente, Email, CodigoProducto, Comentario, Puntaje, Rating, FechaOrigen
        FROM Staging.StgOpiniones
        WHERE EstadoCarga = 'NEW';

        DECLARE @rows INT = (SELECT COUNT(1) FROM #ToProcess);
        IF @rows = 0
        BEGIN
            INSERT INTO Logging.AuditoriaETL(ProcesoETL, Fuente, RegistrosProcesados, FechaInicio, FechaFin, Estado, MensajeError)
            VALUES('LoadFactOpiniones', 'Staging', 0, @start, GETDATE(), 'SKIPPED', 'No NEW rows');
            RETURN 0;
        END

        -- Insert mapping: try to find dimension ids; if not found, NULLs will be inserted
        INSERT INTO Fact.FactOpiniones (
            Opinion_id, Tiempo_id, Producto_id, Cliente_id, Fuente_id, Sentimiento_id, ID_Opinion_Original,
            Comentario, Puntaje_Satisfaccion, Rating, Longitud_Comentario, Tiene_Comentario, Palabras_clavePositivas,
            Palabras_claveNegativas, Score_sentimiento, Hash_Comentario, Fecha_carga
        )
        SELECT
            (SELECT ISNULL(MAX(Opinion_id),0) + ROW_NUMBER() OVER (ORDER BY t.StgId) FROM Fact.FactOpiniones) + rn AS Opinion_id,
            dt.Tiempo_id,
            p.Producto_id,
            c.Cliente_id,
            f.Fuente_id,
            s.Sentimiento_id,
            t.IdOrigen,
            t.Comentario,
            t.Puntaje,
            t.Rating,
            LEN(ISNULL(t.Comentario,'')),
            CASE WHEN LEN(ISNULL(t.Comentario,'')) > 0 THEN 1 ELSE 0 END,
            NULL,
            NULL,
            NULL,
            LTRIM(RTRIM(REPLACE(sys.fn_varbintohexstr(HASHBYTES('MD5', ISNULL(t.Comentario,''))), '0x', ''))),
            GETDATE()
        FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY StgId) AS rn FROM #ToProcess
        ) t
        LEFT JOIN Dimension.DimProducto p ON p.Id_Producto_Origen = t.CodigoProducto
        LEFT JOIN Dimension.DimCliente c ON c.Email = t.Email OR c.Id_Cliente_origen = t.ClienteId
        LEFT JOIN Dimension.DimFuente f ON f.Tipo_fuente = t.Fuente
        LEFT JOIN Dimension.DimTiempo dt ON dt.Fecha = CONVERT(date, t.FechaOrigen)
        LEFT JOIN Dimension.DimSentimiento s ON s.Clasificacion =
            (CASE WHEN t.Puntaje IS NOT NULL AND t.Puntaje >= 70 THEN 'Positive' WHEN t.Puntaje IS NOT NULL AND t.Puntaje >= 40 THEN 'Neutral' ELSE 'Negative' END);

        -- Mark processed in staging
        UPDATE s
        SET EstadoCarga = 'PROCESSED', FechaActualizacion = GETDATE()
        FROM Staging.StgOpiniones s
        INNER JOIN #ToProcess t ON s.StgId = t.StgId;

        INSERT INTO Logging.AuditoriaETL(ProcesoETL, Fuente, RegistrosProcesados, FechaInicio, FechaFin, Estado, MensajeError)
        VALUES('LoadFactOpiniones', 'Staging', @rows, @start, GETDATE(), 'COMPLETED', NULL);

        COMMIT TRANSACTION;
        RETURN 0;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        DECLARE @err NVARCHAR(MAX) = ERROR_MESSAGE();
        INSERT INTO Logging.AuditoriaETL(ProcesoETL, Fuente, RegistrosProcesados, FechaInicio, FechaFin, Estado, MensajeError)
        VALUES('LoadFactOpiniones', 'Staging', 0, @start, GETDATE(), 'FAILED', @err);
        RAISERROR('LoadFactOpiniones failed: %s', 16, 1, @err);
        RETURN -1;
    END CATCH
END
GO
