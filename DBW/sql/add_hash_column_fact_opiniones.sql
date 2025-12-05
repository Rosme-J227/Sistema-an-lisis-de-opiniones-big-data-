USE DWOpinionClientes;
GO

IF OBJECT_ID('Fact.FactOpiniones','U') IS NOT NULL
BEGIN
    IF COL_LENGTH('Fact.FactOpiniones', 'Hash_Comentario') IS NULL
    BEGIN
        PRINT 'Adding column Hash_Comentario to Fact.FactOpiniones';
        ALTER TABLE Fact.FactOpiniones ADD Hash_Comentario NVARCHAR(100) NULL;
        PRINT 'Column Hash_Comentario added.';
    END
    ELSE
    BEGIN
        PRINT 'Column Hash_Comentario already exists.';
    END
END
ELSE
BEGIN
    PRINT 'Table Fact.FactOpiniones does not exist in DWOpinionClientes. Please create the table first.';
END
GO
