Param(
    [string]$ConnectionString = "Server=localhost,1433;Database=DWOpinionClientes;Trusted_Connection=True;TrustServerCertificate=True;",
    [string]$CsvLoaderProject = "tools\csv_bulk_loader\CsvBulkLoader.csproj",
    [string]$WorkerProject = "src\CustomerOpinionsETL.Worker\CustomerOpinionsETL.Worker.csproj",
    [string]$DiagnosticsScript = "DBW\sql\dw_diagnostics.sql"
)

Write-Host "Using connection string: $ConnectionString"


$env:ConnectionStrings__DWOpinionClientes = $ConnectionString


$env:ConnectionStrings__SourceOpiniones = $ConnectionString


Write-Host "Running fast DW populator (fills Dimension tables)..."
Push-Location (Join-Path (Split-Path $PSScriptRoot) 'tools\fast_populate_dw')
dotnet restore --verbosity minimal
dotnet run --project FastPopulateDw.csproj -- $ConnectionString 2>&1 | Tee-Object -FilePath "$(Join-Path (Get-Location) 'fast_populate_dw_log.txt')"
Pop-Location


Write-Host "Running Source DB seeder..."
Push-Location (Join-Path (Split-Path $PSScriptRoot) 'tools\seed_source_db')
dotnet restore --verbosity minimal
dotnet run --project SeedSourceDb.csproj -- $ConnectionString 2>&1 | Tee-Object -FilePath "$(Join-Path (Get-Location) 'seed_source_db_log.txt')"
Pop-Location


Write-Host "Running CSV bulk loader..."
Push-Location (Split-Path $CsvLoaderProject)
dotnet restore --verbosity minimal
dotnet run --project (Split-Path $CsvLoaderProject -Leaf) 2>&1 | Tee-Object -FilePath "$(Join-Path (Get-Location) 'csv_load_log.txt')"
Pop-Location


Write-Host "Running Worker (ETL) to process staging..."
Push-Location (Split-Path $WorkerProject)
dotnet restore --verbosity minimal
dotnet run --project (Split-Path $WorkerProject -Leaf) 2>&1 | Tee-Object -FilePath "$(Join-Path (Get-Location) 'worker_run_log.txt')"
Pop-Location


Write-Host "Running DW diagnostics..."
$diagOut = Join-Path (Resolve-Path "$PSScriptRoot\..\DBW\sql") 'dw_diagnostics_output.txt'
sqlcmd -S localhost,1433 -i $DiagnosticsScript -o $diagOut
Write-Host "Diagnostics written to: $diagOut"

Write-Host "All steps finished. Check csv_load_log.txt, worker_run_log.txt and dw_diagnostics_output.txt for details."
