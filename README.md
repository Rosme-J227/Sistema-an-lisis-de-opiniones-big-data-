# Sistema de Análisis de Opiniones 

Resumen
-------
Este repositorio contiene un pipeline ETL que extrae opiniones de clientes desde fuentes locales (CSV / base origen / API), normaliza y carga dimensiones y hechos en un Data Warehouse pensado para alimentar un dashboard con KPIs auditables.

Objetivo inmediato
------------------
Proveer scripts y utilidades para:
- Limpiar las tablas de hechos del DWH antes de una carga controlada.
- Cargar (bulk) la tabla de hechos `FactOpiniones` desde la tabla de staging.
- Disponer pasos reproducibles para ejecutar la carga rápida o el proceso formal vía Worker.


