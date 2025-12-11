-- Script para crear login y usuario SQL para la app
-- Ejecutar como administrador en el servidor SQL (p. ej. con sqlcmd -E o en SSMS)
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'app_user')
BEGIN
    CREATE LOGIN app_user WITH PASSWORD = 'AppUser#2025!';
END
GO
USE DWOpinionClientes;
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'app_user')
BEGIN
    CREATE USER app_user FOR LOGIN app_user;
    ALTER ROLE db_datareader ADD MEMBER app_user;
END
GO
-- Opcional: conceder permisos adicionales si los necesitas
-- ALTER ROLE db_datawriter ADD MEMBER app_user;

PRINT 'OK: login/app_user created or already exists';
