**Sistema de Análisis de Opiniones de Clientes — Documento de Requisitos (SRS)**

**1. Introducción**

1.1 Propósito

Este documento recoge los requisitos funcionales y no funcionales del Sistema de Análisis de Opiniones de Clientes con Proceso ETL. Define el alcance, actores, funciones del sistema y los entregables esperados.

1.2 Alcance

El sistema permitirá a la organización consolidar opiniones de múltiples fuentes (CSV, bases de datos relacionales y API REST), ejecutar un pipeline ETL para limpiar y clasificar opiniones, almacenar resultados en una base analítica y exponer vistas para consumo por Power BI u otras plataformas de BI.

1.3 Definiciones, Acrónimos y Abreviaturas

- ETL: Extract, Transform, Load.
- API REST: Interfaz HTTP/JSON.
- NLP: Natural Language Processing.
- KPI: Key Performance Indicator (Indicador clave de desempeño).

1.4 Referencias

- .NET 8 Worker Service pattern
- ML.NET (opcional) para clasificación de texto
- Power BI Desktop / Service

**2. Descripción General del Sistema**

2.1 Perspectiva del Producto

Componentes principales:
- Módulo ETL: .NET Worker Service que orquesta extracción, transformaciones y carga.
- Data Warehouse (SQL Server preferido en este repo): esquemas Staging, Dimension, Fact.
- Capa de presentación: Power BI (preferido) o app web ASP.NET Core.

2.2 Funciones del Sistema

- Extracción de datos de CSV, BD relacional y API REST.
- Transformación: limpieza, deduplicación, normalización de fechas/entidades, clasificación de sentimiento y cálculo de métricas.
- Carga: upsert a dimensiones y carga de hechos.
- Exposición de vistas preparadas para BI y generación de KPIs.

2.3 Usuarios del Sistema

- Analistas de negocio: crean y consumen dashboards.
- Gerencia: revisa KPIs estratégicos.
- Equipo de TI/DevOps: despliega y mantiene el pipeline.

2.4 Restricciones

- Implementación base en .NET 8.
- Persistencia en base SQL compatible (SQL Server en el repositorio actual).
- Clasificación inicial con enfoque lexicón o modelo ML sencillo.

**3. Requisitos Específicos**

3.1 Requisitos Funcionales (RF)

RF1: Extracción
- RF1.1: El sistema debe aceptar archivos CSV, consultas a BD relacional y endpoints REST.

RF2: Transformación
- RF2.1: El sistema debe eliminar duplicados por hash de comentario o combinación de campos clave.
- RF2.2: Debe filtrar comentarios vacíos o nulos.
- RF2.3: Debe normalizar fechas a UTC y mapear productos/clientes por clave.
- RF2.4: Debe clasificar opiniones en Positiva/Neutra/Negativa (lexicón configurable y/o modelo ML).

RF3: Carga
- RF3.1: El sistema debe upsert a tablas de dimensiones (`DimProducto`, `DimCliente`, `DimTiempo`, `DimFuente`).
- RF3.2: El sistema debe insertar hechos en `FactOpiniones` con claves hacia dimensiones y métricas calculadas.

RF4: Consultas y reportes
- RF4.1: Exponer vistas para:
  - Opiniones detalladas por producto/cliente/fecha
  - KPIs mensuales por producto
  - Top productos por volumen y satisfacción

RF5: Operaciones
- RF5.1: Permitir re-ejecución idempotente del ETL para evitar duplicados.
- RF5.2: Registrar logs de operaciones y errores.

3.2 Requisitos No Funcionales (RNF)

RNF1: Rendimiento
- RNF1.1: El pipeline debe poder procesar 50,000 comentarios en menos de 5 minutos en hardware razonable (especificar infra en runbook).

RNF2: Escalabilidad
- RNF2.1: El diseño debe permitir añadir nuevas fuentes sin rediseñar la arquitectura.

RNF3: Seguridad
- RNF3.1: Credenciales de BD y API deben gestionarse con secret store (Key Vault, local secrets, o variables de entorno).
- RNF3.2: Conexiones a servicios externos deben usar HTTPS.

RNF4: Usabilidad
- RNF4.1: El dashboard debe ser intuitivo para usuarios no técnicos.

RNF5: Mantenibilidad
- RNF5.1: Código organizado en proyectos (Core, Infrastructure, Worker, Dashboard). Tests unitarios y documentación mínima para runbook.

**4. Modelo de Datos (Resumen)**

Tablas principales (nombres prácticos usados en el repo):
- `DimCliente` (ClienteId PK, Nombre, Email, Otros)
- `DimProducto` (ProductoId PK, Nombre, Categoria)
- `DimFuente` (FuenteId PK, TipoFuente, Nombre)
- `DimTiempo` (TiempoId PK, Fecha, Año, Mes, Dia, MesAño)
- `FactOpiniones` (OpinionId PK, ClienteId FK, ProductoId FK, FuenteId FK, TiempoId FK, Comentario, Clasificacion, PuntajeSatisfaccion, HashComentario, FechaCarga)

Clave: usar surrogate keys en dimensiones, y asegurarse de índices en claves FK para consultas rápidas.

**5. Entregables**

- Código fuente en C# (.NET Worker Service): `src/CustomerOpinionsETL.*`.
- Scripts SQL para crear esquema y vistas (`sql/00_*.sql`, `sql/vw_*.sql`).
- Diagrama ER en PlantUML: `docs/er_diagram.puml`.
- Diagrama de flujo ETL en PlantUML: `docs/etl_flow.puml`.
- Documentación de uso y runbook: `docs/powerbi_quickstart.md`, `docs/SRS.md`.
- Opcional: `.pbix` de ejemplo y runbook de despliegue para Power BI Service.

---

Versión y control
- Autor: Equipo ETL / Maintainer del repositorio
- Fecha: (ver control de versiones en Git)
- Estado: Borrador — pendiente validación por stakeholders
