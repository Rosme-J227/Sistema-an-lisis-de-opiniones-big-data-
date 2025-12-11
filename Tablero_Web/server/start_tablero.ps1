<#
Start-Tablero.ps1
Script interactivo para preparar y arrancar el Tablero_Web.

Características:
- Genera `.env` con credenciales (SQL auth) o activa Windows auth
- Opcional: crear un login SQL (`app_user`) ejecutando comandos T-SQL con `sqlcmd`
- Ejecuta `npm install` si es necesario y arranca el servidor en un proceso separado
- Abre el navegador en http://localhost:3000

Uso: ejecutar este script en PowerShell con permisos adecuados.
#>

Set-StrictMode -Version Latest
cd $PSScriptRoot

Write-Host "Este script te guiará para configurar y arrancar el Tablero_Web." -ForegroundColor Cyan

$choice = Read-Host "Elige modo de conexión: (1) SQL auth (recomendado) (2) Windows auth (msnodesqlv8) (3) Crear login SQL y usarlo (1/2/3)"
if ($choice -eq '3') { $choice = '1'; $createLogin = $true } else { $createLogin = $false }

if ($choice -eq '1') {
  $dbServer = Read-Host "DB_SERVER (por defecto: localhost)"
  if (-not $dbServer) { $dbServer = 'localhost' }
  $dbPort = Read-Host "DB_PORT (por defecto: 1433)"
  if (-not $dbPort) { $dbPort = '1433' }
  $dbName = Read-Host "DB_DATABASE (por defecto: DWOpinionClientes)"
  if (-not $dbName) { $dbName = 'DWOpinionClientes' }
  if ($createLogin) {
    $dbUser = Read-Host "Nuevo DB_USER a crear (ej: app_user)"
    $dbPassword = Read-Host "Password para el nuevo usuario (se mostrará en claro)"
  } else {
    $dbUser = Read-Host "DB_USER (ej: sa o usuario existente)"
    $dbPassword = Read-Host "DB_PASSWORD"
  }
  $envContent = @"
DB_SERVER=$dbServer
DB_PORT=$dbPort
DB_DATABASE=$dbName
DB_USER=$dbUser
DB_PASSWORD=$dbPassword
PORT=3000
"@
  $envContent | Out-File -Encoding utf8 .env
  Write-Host ".env creado con usuario '$dbUser' (revisa el archivo antes de iniciar)." -ForegroundColor Green

  if ($createLogin) {
    Write-Host "Creando login y usuario en la base de datos (requiere permisos)." -ForegroundColor Yellow
    $sql = "IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = '$dbUser')\nCREATE LOGIN [$dbUser] WITH PASSWORD = N'$dbPassword';\nUSE [$dbName];\nIF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = '$dbUser')\nCREATE USER [$dbUser] FOR LOGIN [$dbUser];\nEXEC sp_addrolemember N'db_datareader', N'$dbUser';"
    try {
      sqlcmd -S "$dbServer,$dbPort" -E -Q $sql -b
      Write-Host "Login y usuario creados (o ya existían)." -ForegroundColor Green
    } catch {
      Write-Host "Fallo al crear login SQL con sqlcmd. Asegúrate de ejecutar PowerShell como administrador o verifica permisos." -ForegroundColor Red
      Write-Host "Error: $_"
    }
  }

} elseif ($choice -eq '2') {
  Write-Host "Configurando autenticación Windows (msnodesqlv8)." -ForegroundColor Cyan
  $envContent = @"
DB_SERVER=localhost
DB_PORT=1433
DB_DATABASE=DWOpinionClientes
USE_WINDOWS_AUTH=true
PORT=3000
"@
  $envContent | Out-File -Encoding utf8 .env
  Write-Host ".env creado para Windows Auth. Se intentará usar driver msnodesqlv8." -ForegroundColor Green
  Write-Host "Instalando dependencia msnodesqlv8 (puede tardar)." -ForegroundColor Yellow
  npm install msnodesqlv8
} else {
  Write-Host "Opción no válida. Salir." -ForegroundColor Red
  exit 1
}

# Instalar dependencias si node_modules no existe
if (-not (Test-Path node_modules)) {
  Write-Host "Instalando dependencias npm..." -ForegroundColor Cyan
  npm install
}

Write-Host "Arrancando servidor en background..." -ForegroundColor Cyan
# Lanzar npm start en un proceso separado
Start-Process -FilePath npm -ArgumentList 'start' -WorkingDirectory $PSScriptRoot

Start-Sleep -Seconds 2
Write-Host "Abriendo navegador en http://localhost:3000" -ForegroundColor Cyan
Start-Process "http://localhost:3000"

Write-Host "Listo. Revisa la consola donde se lanzó 'npm start' para ver logs. Si hay errores, pégalos aquí y te ayudo a corregirlos." -ForegroundColor Green
