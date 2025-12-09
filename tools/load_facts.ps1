<#
Usage: .\load_facts.ps1 -ServerInstance "localhost" -Database "DWOpinionClientes"

This script executes the stored procedure Fact.sp_LoadFactOpiniones on the target SQL Server.
It uses `sqlcmd`. If sqlcmd is not available, run the T-SQL directly from your SQL client.
#>

param(
    [string]$ServerInstance = "localhost",
    [string]$Database = "DWOpinionClientes"
)

Write-Host "Running load for FactOpiniones against $ServerInstance/$Database"

$tsql = "EXEC Fact.sp_LoadFactOpiniones;"

if (Get-Command sqlcmd -ErrorAction SilentlyContinue) {
    sqlcmd -S $ServerInstance -d $Database -Q $tsql -b
    if ($LASTEXITCODE -ne 0) { Write-Error "sqlcmd returned exit code $LASTEXITCODE"; exit $LASTEXITCODE }
} else {
    Write-Host "sqlcmd not found. Please run the following T-SQL on the target database:" -ForegroundColor Yellow
    Write-Host $tsql
}

Write-Host "Done. Check Logging.AuditoriaETL for audit records." -ForegroundColor Green
