-- Cambia la contraseña del login app_user para que coincida con .env (sin '#')
ALTER LOGIN app_user WITH PASSWORD = 'AppUser2025!';
GO
PRINT 'OK: password changed';
