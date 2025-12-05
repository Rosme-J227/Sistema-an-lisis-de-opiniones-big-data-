
IF DB_ID('SourceOpiniones') IS NOT NULL
    DROP DATABASE SourceOpiniones;
GO
CREATE DATABASE SourceOpiniones;
GO
USE SourceOpiniones;
GO
CREATE TABLE Clientes (
    ClienteId INT IDENTITY(1,1) PRIMARY KEY,
    Id_Cliente_Origen NVARCHAR(50) NULL,
    Nombre NVARCHAR(150),
    Email NVARCHAR(150),
    Pais NVARCHAR(100),
    RangoEdad NVARCHAR(50),
    Segmento NVARCHAR(50)
);
CREATE TABLE Productos (
    ProductoId INT IDENTITY(1,1) PRIMARY KEY,
    Id_Producto_Origen NVARCHAR(50) NULL,
    Nombre NVARCHAR(200),
    Categoria NVARCHAR(100),
    FechaAlta DATE,
    Activo BIT
);
CREATE TABLE Reviews (
    ReviewId BIGINT IDENTITY(1,1) PRIMARY KEY,
    IdOrigen NVARCHAR(100) NULL,
    ClienteId INT NULL,
    ProductoId INT NULL,
    Fuente NVARCHAR(50),
    Fecha DATETIME,
    Comentario NVARCHAR(MAX),
    Puntaje INT NULL,
    Rating INT NULL
);
GO
