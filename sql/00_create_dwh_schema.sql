

USE DWOpinionClientes;
GO
IF SCHEMA_ID('Staging') IS NULL
    EXEC ('CREATE SCHEMA Staging');
GO
IF OBJECT_ID('Staging.StgOpiniones') IS NULL
CREATE TABLE Staging.StgOpiniones (
    StgId BIGINT IDENTITY(1,1) PRIMARY KEY,
    Fuente NVARCHAR(50),
    IdOrigen NVARCHAR(100),
    ClienteId INT NULL,
    NombreCliente NVARCHAR(200),
    Email NVARCHAR(200),
    CodigoProducto NVARCHAR(50),
    Comentario NVARCHAR(MAX),
    Puntaje INT NULL,
    Rating INT NULL,
    FechaOrigen DATETIME,
    FechaCarga DATETIME DEFAULT GETDATE(),
    EstadoCarga NVARCHAR(20) DEFAULT 'NEW'
);
GO
