# Tablero_Web

Pequeño tablero web que consume la vista `vw_AnaliticaOpiniones` y muestra 4 visualizaciones:

- Opiniones por producto y fecha (Top N)
- Número de comentarios procesados
- Porcentaje de satisfacción por producto (Rating >= 4)
- Tendencia de satisfacción en el tiempo

Requisitos: `Node.js` (v16+ recomendado) y acceso a la base de datos SQL Server.

Instalación y ejecución (PowerShell):

```powershell
cd "c:\Users\marielys j\Downloads\CustomerOpinionsETL_Project\Tablero_Web\server"
npm install
copy .env.example .env
# Edita .env y ajusta la conexión (o define DB_CONNECTION_STRING)
npm start
```


Abre en el navegador: `http://localhost:3000`

Toggle de definición de "satisfecho": el tablero permite alternar entre dos definiciones desde la UI:
- `Rating >= 4` (por defecto)
- `Score_sentimiento >= 0.5`

También puedes ajustar el umbral desde la API pasando `?definition=score&threshold=0.6` o `?definition=rating&threshold=3` en los endpoints relevantes.

Notas:
- Si tu instancia usa autenticación Windows, puedes construir `DB_CONNECTION_STRING` adecuada o ejecutar con credenciales SQL.
- El servidor también sirve los archivos estáticos en `Tablero_Web/web`.
 
Automatización rápida:
- Si no quieres editar manualmente, ejecuta `start_tablero.ps1` dentro de `Tablero_Web/server`. El script te preguntará si deseas usar autenticación SQL o Windows, puede crear un login SQL opcional, instalar dependencias e iniciar el servidor automáticamente.

```powershell
cd "c:\Users\marielys j\Downloads\CustomerOpinionsETL_Project\Tablero_Web\server"
.\start_tablero.ps1
```
