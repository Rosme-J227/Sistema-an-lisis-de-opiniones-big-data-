USE DWOpinionClientes;
GO

/*
Procedure: Fact.sp_CleanFactTables
Purpose: Clean (truncate/delete) fact tables in the Fact schema before loading.
Usage examples:
  EXEC Fact.sp_CleanFactTables; -- cleans all Fact.* tables using TRUNCATE when possible
  EXEC Fact.sp_CleanFactTables @Target = 'FactOpiniones', @Mode = 'DELETE'; -- clean single table
Notes: The procedure logs each action into Logging.AuditoriaETL.
*/
CREATE OR ALTER PROCEDURE Fact.sp_CleanFactTables
    @Target NVARCHAR(200) = 'ALL',
    @Mode NVARCHAR(10) = 'TRUNCATE' -- or 'DELETE'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @start DATETIME = GETDATE();

    BEGIN TRY
        DECLARE @tables TABLE (TableName NVARCHAR(256));

        IF UPPER(LTRIM(RTRIM(@Target))) = 'ALL'
        BEGIN
            INSERT INTO @tables(TableName)
            SELECT QUOTENAME(SCHEMA_NAME(schema_id)) + '.' + QUOTENAME(name)
            FROM sys.tables
            WHERE schema_id = SCHEMA_ID('Fact')
              AND name LIKE 'Fact%';
        END
        ELSE
        BEGIN
            -- allow passing either 'Fact.Table' or just 'FactOpiniones'
            DECLARE @t NVARCHAR(256) = @Target;
            IF CHARINDEX('.', @t) = 0
                SET @t = QUOTENAME('Fact') + '.' + QUOTENAME(@t);
            INSERT INTO @tables(TableName) VALUES (@t);
        END

        DECLARE @tbl NVARCHAR(256);
        DECLARE @beforeRows BIGINT;
        DECLARE @afterRows BIGINT;
        DECLARE @actionTaken NVARCHAR(20);

        DECLARE cur CURSOR LOCAL FOR SELECT TableName FROM @tables;
        OPEN cur;
        FETCH NEXT FROM cur INTO @tbl;
        WHILE @@FETCH_STATUS = 0
        BEGIN
            IF OBJECT_ID(@tbl) IS NULL
            BEGIN
                INSERT INTO Logging.AuditoriaETL(ProcesoETL, Fuente, RegistrosProcesados, FechaInicio, FechaFin, Estado, MensajeError)
                VALUES('CleanFacts', @tbl, 0, @start, GETDATE(), 'SKIPPED', 'Table not found');
                FETCH NEXT FROM cur INTO @tbl; CONTINUE;
            END

            SET @beforeRows = 0; SET @afterRows = 0; SET @actionTaken = '';
            EXEC sp_executesql N'SELECT @cnt = COUNT(1) FROM ' + @tbl, N'@cnt BIGINT OUTPUT', @beforeRows OUTPUT;

            IF UPPER(@Mode) = 'TRUNCATE'
            BEGIN
                -- Can we truncate? TRUNCATE fails if other tables reference this table via FK.
                IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys fk WHERE fk.referenced_object_id = OBJECT_ID(@tbl))
                BEGIN
                    EXEC('TRUNCATE TABLE ' + @tbl);
                    SET @actionTaken = 'TRUNCATE';
                END
                ELSE
                BEGIN
                    EXEC('DELETE FROM ' + @tbl);
                    SET @actionTaken = 'DELETE';
                END
            END
            ELSE
            BEGIN
                EXEC('DELETE FROM ' + @tbl);
                SET @actionTaken = 'DELETE';
            END

            EXEC sp_executesql N'SELECT @cnt = COUNT(1) FROM ' + @tbl, N'@cnt BIGINT OUTPUT', @afterRows OUTPUT;

            INSERT INTO Logging.AuditoriaETL(ProcesoETL, Fuente, RegistrosProcesados, FechaInicio, FechaFin, Estado, MensajeError)
            VALUES('CleanFacts', @tbl, ISNULL(@beforeRows,0) - ISNULL(@afterRows,0), @start, GETDATE(), 'COMPLETED', @actionTaken);

            FETCH NEXT FROM cur INTO @tbl;
        END
        CLOSE cur; DEALLOCATE cur;

        RETURN 0;
    END TRY
    BEGIN CATCH
        DECLARE @errMsg NVARCHAR(MAX) = ERROR_MESSAGE();
        INSERT INTO Logging.AuditoriaETL(ProcesoETL, Fuente, RegistrosProcesados, FechaInicio, FechaFin, Estado, MensajeError)
        VALUES('CleanFacts', @Target, 0, @start, GETDATE(), 'FAILED', @errMsg);
        RAISERROR('Error cleaning fact tables: %s', 16, 1, @errMsg);
        RETURN -1;
    END CATCH
END
GO

/* Helper: example usage
-- Clean all fact tables using TRUNCATE when possible
EXEC Fact.sp_CleanFactTables;

-- Clean specific table using DELETE
EXEC Fact.sp_CleanFactTables @Target = 'FactOpiniones', @Mode = 'DELETE';
*/
