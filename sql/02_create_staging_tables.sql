




USE [DWOpinionClientes]
GO




IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Staging')
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA Staging'
END
GO





IF OBJECT_ID('Staging.StgOpiniones', 'U') IS NOT NULL
    DROP TABLE Staging.StgOpiniones;
GO

CREATE TABLE Staging.StgOpiniones (
    StgId BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    Fuente NVARCHAR(50) NOT NULL,
    IdOrigen NVARCHAR(100) NULL,
    ClienteId INT NULL,
    NombreCliente NVARCHAR(100) NULL,
    Email NVARCHAR(100) NULL,
    CodigoProducto NVARCHAR(100) NULL,
    Comentario NVARCHAR(MAX) NULL,
    Puntaje INT NULL,                                
    Rating INT NULL,
    FechaOrigen DATETIME NOT NULL,
    EstadoCarga NVARCHAR(20) NOT NULL DEFAULT 'NEW',
    MensajeError NVARCHAR(500) NULL,
    Hash_MD5 NVARCHAR(32) NULL,
    FechaCarga DATETIME NOT NULL DEFAULT GETDATE(),
    FechaActualizacion DATETIME NULL,               
    CONSTRAINT UQ_StgOpiniones_IdOrigen UNIQUE (Fuente, IdOrigen)
);


CREATE NONCLUSTERED INDEX IX_StgOpiniones_EstadoCarga ON Staging.StgOpiniones (EstadoCarga)
    WHERE EstadoCarga IN ('NEW', 'PROCESSING');

CREATE NONCLUSTERED INDEX IX_StgOpiniones_FechaOrigen ON Staging.StgOpiniones (FechaOrigen);

CREATE NONCLUSTERED INDEX IX_StgOpiniones_Email ON Staging.StgOpiniones (Email) WHERE Email IS NOT NULL;

CREATE NONCLUSTERED INDEX IX_StgOpiniones_CodigoProducto ON Staging.StgOpiniones (CodigoProducto);

GO


IF OBJECT_ID('Staging.StgProductos', 'U') IS NOT NULL
    DROP TABLE Staging.StgProductos;
GO

CREATE TABLE Staging.StgProductos (
    StgProductoId INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    CodigoProducto NVARCHAR(100) NOT NULL,
    NombreProducto NVARCHAR(200) NULL,
    Categoria NVARCHAR(100) NULL,
    Descripcion NVARCHAR(500) NULL,
    Estado NVARCHAR(20) NOT NULL DEFAULT 'NEW', 
    MensajeError NVARCHAR(500) NULL,
    FechaCarga DATETIME NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_StgProductos_Codigo UNIQUE (CodigoProducto)
);

CREATE NONCLUSTERED INDEX IX_StgProductos_Estado ON Staging.StgProductos (Estado);

GO


IF OBJECT_ID('Staging.StgClientes', 'U') IS NOT NULL
    DROP TABLE Staging.StgClientes;
GO

CREATE TABLE Staging.StgClientes (
    StgClienteId INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    Email NVARCHAR(100) NOT NULL,
    NombreCliente NVARCHAR(100) NULL,
    Pais NVARCHAR(100) NULL,
    RangoEdad NVARCHAR(50) NULL,
    Segmento NVARCHAR(50) NULL,
    Estado NVARCHAR(20) NOT NULL DEFAULT 'NEW',
    MensajeError NVARCHAR(500) NULL,
    FechaCarga DATETIME NOT NULL DEFAULT GETDATE(),
    CONSTRAINT UQ_StgClientes_Email UNIQUE (Email)
);

CREATE NONCLUSTERED INDEX IX_StgClientes_Estado ON Staging.StgClientes (Estado);

GO


IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Logging')
BEGIN
    EXEC sp_executesql N'CREATE SCHEMA Logging'
END
GO

IF OBJECT_ID('Logging.AuditoriaETL', 'U') IS NOT NULL
    DROP TABLE Logging.AuditoriaETL;
GO

CREATE TABLE Logging.AuditoriaETL (
    AuditoriaId BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    ProcesoETL NVARCHAR(100) NOT NULL,               
    Fuente NVARCHAR(100) NOT NULL,                   
    RegistrosProcesados INT NOT NULL DEFAULT 0,
    RegistrosError INT NOT NULL DEFAULT 0,
    RegistrosDuplicados INT NOT NULL DEFAULT 0,
    RegistrosDescartados INT NOT NULL DEFAULT 0,
    TiempoEjecucionSegundos DECIMAL(10,2) NULL,
    FechaInicio DATETIME NOT NULL DEFAULT GETDATE(),
    FechaFin DATETIME NULL,
    Estado NVARCHAR(20) NOT NULL DEFAULT 'IN_PROGRESS', 
    MensajeError NVARCHAR(500) NULL
);

CREATE NONCLUSTERED INDEX IX_AuditoriaETL_FechaInicio ON Logging.AuditoriaETL (FechaInicio DESC);

CREATE NONCLUSTERED INDEX IX_AuditoriaETL_ProcesoFuente ON Logging.AuditoriaETL (ProcesoETL, Fuente);

GO





IF OBJECT_ID('Logging.ErroresETL', 'U') IS NOT NULL
    DROP TABLE Logging.ErroresETL;
GO

CREATE TABLE Logging.ErroresETL (
    ErrorId BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    AuditoriaId BIGINT NOT NULL,
    TipoError NVARCHAR(100) NOT NULL,
    Mensaje NVARCHAR(MAX) NOT NULL,
    ValorProblematico NVARCHAR(MAX) NULL,
    Fila INT NULL,
    FechaRegistro DATETIME NOT NULL DEFAULT GETDATE(),
    CONSTRAINT FK_ErroresETL_Auditoria FOREIGN KEY (AuditoriaId) 
        REFERENCES Logging.AuditoriaETL(AuditoriaId) ON DELETE CASCADE
);

CREATE NONCLUSTERED INDEX IX_ErroresETL_AuditoriaId ON Logging.ErroresETL (AuditoriaId);

GO




CREATE OR ALTER VIEW Staging.vw_OpinionesNuevas AS
SELECT 
    StgId,
    Fuente,
    IdOrigen,
    ClienteId,
    NombreCliente,
    Email,
    CodigoProducto,
    Comentario,
    Puntaje,
    Rating,
    FechaOrigen,
    FechaCarga
FROM Staging.StgOpiniones
WHERE EstadoCarga = 'NEW'
GO




CREATE OR ALTER PROCEDURE Staging.sp_LimpiarOpinionasAntiguas
    @DiasProcesados INT = 30
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        DELETE FROM Staging.StgOpiniones
        WHERE EstadoCarga = 'PROCESSED'
        AND FechaCarga < DATEADD(DAY, -@DiasProcesados, GETDATE())
        
        RETURN 0
    END TRY
    BEGIN CATCH
        RAISERROR('Error al limpiar staging: %s', 16, 1, ERROR_MESSAGE())
        RETURN -1
    END CATCH
END
GO





CREATE OR ALTER PROCEDURE Staging.sp_MostrarEstadisticasStaging
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        'Opiniones' AS TipoRegistro,
        COUNT(*) AS Total,
        SUM(CASE WHEN EstadoCarga = 'NEW' THEN 1 ELSE 0 END) AS Nuevas,
        SUM(CASE WHEN EstadoCarga = 'PROCESSING' THEN 1 ELSE 0 END) AS EnProceso,
        SUM(CASE WHEN EstadoCarga = 'PROCESSED' THEN 1 ELSE 0 END) AS Procesadas,
        SUM(CASE WHEN EstadoCarga = 'ERROR' THEN 1 ELSE 0 END) AS Errores
    FROM Staging.StgOpiniones
    
    UNION ALL
    
    SELECT 
        'Productos' AS TipoRegistro,
        COUNT(*) AS Total,
        USE [DWOpinionClientes]
        GO

        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Staging')
        BEGIN
            EXEC sp_executesql N'CREATE SCHEMA Staging'
        END
        GO

        IF OBJECT_ID('Staging.StgOpiniones', 'U') IS NOT NULL
            DROP TABLE Staging.StgOpiniones;
        GO

        CREATE TABLE Staging.StgOpiniones (
            StgId BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
            Fuente NVARCHAR(50) NOT NULL,
            IdOrigen NVARCHAR(100) NULL,
            ClienteId INT NULL,
            NombreCliente NVARCHAR(100) NULL,
            Email NVARCHAR(100) NULL,
            CodigoProducto NVARCHAR(100) NULL,
            Comentario NVARCHAR(MAX) NULL,
            Puntaje INT NULL,
            Rating INT NULL,
            FechaOrigen DATETIME NOT NULL,
            EstadoCarga NVARCHAR(20) NOT NULL DEFAULT 'NEW',
            MensajeError NVARCHAR(500) NULL,
            Hash_MD5 NVARCHAR(32) NULL,
            FechaCarga DATETIME NOT NULL DEFAULT GETDATE(),
            FechaActualizacion DATETIME NULL,
            CONSTRAINT UQ_StgOpiniones_IdOrigen UNIQUE (Fuente, IdOrigen)
        );

        CREATE NONCLUSTERED INDEX IX_StgOpiniones_EstadoCarga ON Staging.StgOpiniones (EstadoCarga)
            WHERE EstadoCarga IN ('NEW', 'PROCESSING');

        CREATE NONCLUSTERED INDEX IX_StgOpiniones_FechaOrigen ON Staging.StgOpiniones (FechaOrigen);

        CREATE NONCLUSTERED INDEX IX_StgOpiniones_Email ON Staging.StgOpiniones (Email) WHERE Email IS NOT NULL;

        CREATE NONCLUSTERED INDEX IX_StgOpiniones_CodigoProducto ON Staging.StgOpiniones (CodigoProducto);

        GO

        IF OBJECT_ID('Staging.StgProductos', 'U') IS NOT NULL
            DROP TABLE Staging.StgProductos;
        GO

        CREATE TABLE Staging.StgProductos (
            StgProductoId INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
            CodigoProducto NVARCHAR(100) NOT NULL,
            NombreProducto NVARCHAR(200) NULL,
            Categoria NVARCHAR(100) NULL,
            Descripcion NVARCHAR(500) NULL,
            Estado NVARCHAR(20) NOT NULL DEFAULT 'NEW',
            MensajeError NVARCHAR(500) NULL,
            FechaCarga DATETIME NOT NULL DEFAULT GETDATE(),
            CONSTRAINT UQ_StgProductos_Codigo UNIQUE (CodigoProducto)
        );

        CREATE NONCLUSTERED INDEX IX_StgProductos_Estado ON Staging.StgProductos (Estado);

        GO

        IF OBJECT_ID('Staging.StgClientes', 'U') IS NOT NULL
            DROP TABLE Staging.StgClientes;
        GO

        CREATE TABLE Staging.StgClientes (
            StgClienteId INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
            Email NVARCHAR(100) NOT NULL,
            NombreCliente NVARCHAR(100) NULL,
            Pais NVARCHAR(100) NULL,
            RangoEdad NVARCHAR(50) NULL,
            Segmento NVARCHAR(50) NULL,
            Estado NVARCHAR(20) NOT NULL DEFAULT 'NEW',
            MensajeError NVARCHAR(500) NULL,
            FechaCarga DATETIME NOT NULL DEFAULT GETDATE(),
            CONSTRAINT UQ_StgClientes_Email UNIQUE (Email)
        );

        CREATE NONCLUSTERED INDEX IX_StgClientes_Estado ON Staging.StgClientes (Estado);

        GO

        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'Logging')
        BEGIN
            EXEC sp_executesql N'CREATE SCHEMA Logging'
        END
        GO

        IF OBJECT_ID('Logging.AuditoriaETL', 'U') IS NOT NULL
            DROP TABLE Logging.AuditoriaETL;
        GO

        CREATE TABLE Logging.AuditoriaETL (
            AuditoriaId BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
            ProcesoETL NVARCHAR(100) NOT NULL,
            Fuente NVARCHAR(100) NOT NULL,
            RegistrosProcesados INT NOT NULL DEFAULT 0,
            RegistrosError INT NOT NULL DEFAULT 0,
            RegistrosDuplicados INT NOT NULL DEFAULT 0,
            RegistrosDescartados INT NOT NULL DEFAULT 0,
            TiempoEjecucionSegundos DECIMAL(10,2) NULL,
            FechaInicio DATETIME NOT NULL DEFAULT GETDATE(),
            FechaFin DATETIME NULL,
            Estado NVARCHAR(20) NOT NULL DEFAULT 'IN_PROGRESS',
            MensajeError NVARCHAR(500) NULL
        );

        CREATE NONCLUSTERED INDEX IX_AuditoriaETL_FechaInicio ON Logging.AuditoriaETL (FechaInicio DESC);

        CREATE NONCLUSTERED INDEX IX_AuditoriaETL_ProcesoFuente ON Logging.AuditoriaETL (ProcesoETL, Fuente);

        GO

        IF OBJECT_ID('Logging.ErroresETL', 'U') IS NOT NULL
            DROP TABLE Logging.ErroresETL;
        GO
        CREATE TABLE Logging.ErroresETL (
            ErrorId BIGINT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
            AuditoriaId BIGINT NOT NULL,
            TipoError NVARCHAR(100) NOT NULL,
            Mensaje NVARCHAR(MAX) NOT NULL,
            ValorProblematico NVARCHAR(MAX) NULL,
            Fila INT NULL,
            FechaRegistro DATETIME NOT NULL DEFAULT GETDATE(),
            CONSTRAINT FK_ErroresETL_Auditoria FOREIGN KEY (AuditoriaId) 
                REFERENCES Logging.AuditoriaETL(AuditoriaId) ON DELETE CASCADE
        );

        CREATE NONCLUSTERED INDEX IX_ErroresETL_AuditoriaId ON Logging.ErroresETL (AuditoriaId);

        GO

        CREATE OR ALTER VIEW Staging.vw_OpinionesNuevas AS
        SELECT 
            StgId,
            Fuente,
            IdOrigen,
            ClienteId,
            NombreCliente,
            Email,
            CodigoProducto,
            Comentario,
            Puntaje,
            Rating,
            FechaOrigen,
            FechaCarga
        FROM Staging.StgOpiniones
        WHERE EstadoCarga = 'NEW'
        GO

        CREATE OR ALTER PROCEDURE Staging.sp_LimpiarOpinionasAntiguas
            @DiasProcesados INT = 30
        AS
        BEGIN
            SET NOCOUNT ON;
    
            BEGIN TRY
                DELETE FROM Staging.StgOpiniones
                WHERE EstadoCarga = 'PROCESSED'
                AND FechaCarga < DATEADD(DAY, -@DiasProcesados, GETDATE())
        
                RETURN 0
            END TRY
            BEGIN CATCH
                RAISERROR('Error al limpiar staging: %s', 16, 1, ERROR_MESSAGE())
                RETURN -1
            END CATCH
        END
        GO

        CREATE OR ALTER PROCEDURE Staging.sp_MostrarEstadisticasStaging
        AS
        BEGIN
            SET NOCOUNT ON;
    
            SELECT 
                'Opiniones' AS TipoRegistro,
                COUNT(*) AS Total,
                SUM(CASE WHEN EstadoCarga = 'NEW' THEN 1 ELSE 0 END) AS Nuevas,
                SUM(CASE WHEN EstadoCarga = 'PROCESSING' THEN 1 ELSE 0 END) AS EnProceso,
                SUM(CASE WHEN EstadoCarga = 'PROCESSED' THEN 1 ELSE 0 END) AS Procesadas,
                SUM(CASE WHEN EstadoCarga = 'ERROR' THEN 1 ELSE 0 END) AS Errores
            FROM Staging.StgOpiniones
    
            UNION ALL
    
            SELECT 
                'Productos' AS TipoRegistro,
                COUNT(*) AS Total,
                SUM(CASE WHEN Estado = 'NEW' THEN 1 ELSE 0 END) AS Nuevas,
                SUM(CASE WHEN Estado = 'PROCESSING' THEN 1 ELSE 0 END) AS EnProceso,
                SUM(CASE WHEN Estado = 'PROCESSED' THEN 1 ELSE 0 END) AS Procesadas,
                SUM(CASE WHEN Estado = 'ERROR' THEN 1 ELSE 0 END) AS Errores
            FROM Staging.StgProductos
    
            UNION ALL
    
            SELECT 
                'Clientes' AS TipoRegistro,
                COUNT(*) AS Total,
                SUM(CASE WHEN Estado = 'NEW' THEN 1 ELSE 0 END) AS Nuevas,
                SUM(CASE WHEN Estado = 'PROCESSING' THEN 1 ELSE 0 END) AS EnProceso,
                SUM(CASE WHEN Estado = 'PROCESSED' THEN 1 ELSE 0 END) AS Procesadas,
                SUM(CASE WHEN Estado = 'ERROR' THEN 1 ELSE 0 END) AS Errores
            FROM Staging.StgClientes
        END
        GO

        PRINT 'âœ“ Tablas de Staging creadas exitosamente'
        EXEC Staging.sp_MostrarEstadisticasStaging
        GO
