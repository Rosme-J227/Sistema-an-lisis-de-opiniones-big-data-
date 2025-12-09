# Sistema de Análisis de Opiniones - Documentación rápida

Resumen
-------
Este repositorio contiene un pipeline ETL que extrae opiniones de clientes desde fuentes locales (CSV / base origen / API), normaliza y carga dimensiones y hechos en un Data Warehouse pensado para alimentar un dashboard con KPIs auditables.

Objetivo inmediato
------------------
Proveer scripts y utilidades para:
- Limpiar las tablas de hechos del DWH antes de una carga controlada.
- Cargar (bulk) la tabla de hechos `FactOpiniones` desde la tabla de staging.
- Disponer pasos reproducibles para ejecutar la carga rápida o el proceso formal vía Worker.

Archivos y comandos útiles
-------------------------
- Limpieza de facts (procedimiento T-SQL): `DBW/sql/clean_and_load_facts.sql` (crea `Fact.sp_CleanFactTables`).
- Helper PowerShell para limpieza: `tools/clean_facts.ps1`.
- Cargador de FactOpiniones: `DBW/sql/load_fact_opiniones.sql` (crea `Fact.sp_LoadFactOpiniones`).
- Helper PowerShell para carga: `tools/load_facts.ps1`.
- Migración (añadir columna hash): `DBW/sql/add_hash_column_fact_opiniones.sql`.

Cómo ejecutar (ejemplos, ejecutar desde la raíz del proyecto)
-----------------------------------------------------------
1) Limpiar las tablas de hechos (recomendado: hacer backup antes):

```powershell
Set-Location -LiteralPath "C:\Users\marielys j\Downloads\CustomerOpinionsETL_Project"
.\tools\clean_facts.ps1 -ServerInstance "localhost" -Database "DWOpinionClientes" -Target "ALL" -Mode "TRUNCATE"
```

Si hay referencias FK que impidan `TRUNCATE`, el procedimiento usa `DELETE` automáticamente o puedes forzar `DELETE` indicando `-Mode "DELETE"`.

2) Cargar las filas de staging en la tabla de hechos:

```powershell
.\tools\load_facts.ps1 -ServerInstance "localhost" -Database "DWOpinionClientes"
```

3) Verificar auditoría y resultados:

```sql
USE DWOpinionClientes;
SELECT TOP 50 * FROM Logging.AuditoriaETL ORDER BY FechaFin DESC;
SELECT 'FactOpiniones' AS TableName, COUNT(*) AS TotalRows, MAX(Fecha_carga) AS LastLoad FROM Fact.FactOpiniones;
```

Notas importantes
-----------------
- El loader incluido es un loader de conveniencia para entrega. En producción recomendamos usar el Worker (.NET) porque aplica reglas idempotentes y lógica adicional (deduplicación por hash, normalización avanzada, manejo de errores granular).
- Antes de ejecutar `TRUNCATE`, asegúrate de tener copia de seguridad si necesitas conservar el historial de hechos.
- Si deseas que el formato del hash calculado en SQL coincida exactamente con el usado por el Worker (.NET), puedo ajustar el cálculo de forma compatible.
- El repositorio ya contiene utilidades para "fast populate" de dimensiones y un seeder con datos sintéticos para pruebas.

¿Qué más quieres que haga?
- Puedo ejecutar los scripts contra tu base si me proporcionas acceso (no recomendado por seguridad aquí), o te puedo guiar para que los ejecutes localmente.
- Puedo añadir integración en `tools/run_etl_and_checks.ps1` para encadenar limpieza → carga rápida/worker → chequeos.
