using System.Data;
using System.Globalization;
using CsvHelper;
using Microsoft.Data.SqlClient;




string? connArg = args.Length > 0 ? args[0] : Environment.GetEnvironmentVariable("ConnectionStrings__DWOpinionClientes");
if (string.IsNullOrWhiteSpace(connArg))
{
    Console.WriteLine("ERROR: provide DW connection string as first argument or env var ConnectionStrings__DWOpinionClientes");
    return 1;
}


string? repoCsv = null;
{
    var dir = new DirectoryInfo(AppContext.BaseDirectory);
    while (dir != null)
    {
        var candidate = Path.Combine(dir.FullName, "csv");
        if (Directory.Exists(candidate)) { repoCsv = candidate; break; }
        dir = dir.Parent;
    }
}
if (string.IsNullOrEmpty(repoCsv))
{
    Console.WriteLine("ERROR: csv folder not found in any parent directories.");
    return 2;
}

var connStr = connArg;
try
{
    using var conn = new SqlConnection(connStr);
    await conn.OpenAsync();


    var clientsFile = Path.Combine(repoCsv, "clients.csv");
    if (File.Exists(clientsFile))
    {
        Console.WriteLine("Populating Dimension.DimCliente...");
        var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT Id_Cliente_origen FROM Dimension.DimCliente";
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                if (r.IsDBNull(0)) { existing.Add(""); continue; }
                var val = r.GetValue(0);
                existing.Add(val?.ToString() ?? "");
            }
        }

        var dt = new DataTable();
        dt.Columns.Add(new DataColumn("Id_Cliente_origen", typeof(int)));
        dt.Columns.Add(new DataColumn("Nombre_Cliente", typeof(string)));
        dt.Columns.Add(new DataColumn("Email", typeof(string)));

        using var sr = new StreamReader(clientsFile);
        using var csv = new CsvReader(sr, CultureInfo.InvariantCulture);
        csv.Read(); csv.ReadHeader();
        while (csv.Read())
        {
            var id = csv.GetField("IdCliente");
            var nombre = csv.GetField("Nombre");
            var email = csv.GetField("Email");
            if (!int.TryParse(id, out var idInt)) continue;
            if (existing.Contains(idInt.ToString())) continue;
            var r = dt.NewRow();
            r["Id_Cliente_origen"] = idInt;
            r["Nombre_Cliente"] = nombre;
            r["Email"] = email;
            dt.Rows.Add(r);
        }

        if (dt.Rows.Count > 0)
        {
            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = "Dimension.DimCliente", BatchSize = 1000 };
            bulk.ColumnMappings.Add("Id_Cliente_origen", "Id_Cliente_origen");
            bulk.ColumnMappings.Add("Nombre_Cliente", "Nombre_Cliente");
            bulk.ColumnMappings.Add("Email", "Email");
            await bulk.WriteToServerAsync(dt);
            Console.WriteLine($"Inserted {dt.Rows.Count} clientes into Dimension.DimCliente");
        }
        else Console.WriteLine("No new clientes to insert.");
    }


    var productsFile = Path.Combine(repoCsv, "products.csv");
    if (File.Exists(productsFile))
    {
        Console.WriteLine("Populating Dimension.DimProducto...");
        var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT Id_Producto_Origen FROM Dimension.DimProducto";
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                if (r.IsDBNull(0)) { existing.Add(""); continue; }
                var val = r.GetValue(0);
                existing.Add(val?.ToString() ?? "");
            }
        }

        var dt = new DataTable();

        dt.Columns.Add(new DataColumn("Producto_id", typeof(int)));
        dt.Columns.Add(new DataColumn("Id_Producto_Origen", typeof(int)));
        dt.Columns.Add(new DataColumn("Nombre_producto", typeof(string)));
        dt.Columns.Add(new DataColumn("Categoria", typeof(string)));

        using var sr = new StreamReader(productsFile);
        using var csv = new CsvReader(sr, CultureInfo.InvariantCulture);
        csv.Read(); csv.ReadHeader();
        while (csv.Read())
        {
            var id = csv.GetField("IdProducto");
            var nombre = csv.GetField("Nombre");
            var categoria = csv.GetField("CategorÃ­a");
            if (!int.TryParse(id, out var idProd)) continue;
            if (existing.Contains(idProd.ToString())) continue;
            var r = dt.NewRow();
            r["Producto_id"] = idProd;
            r["Id_Producto_Origen"] = idProd;
            r["Nombre_producto"] = nombre;
            r["Categoria"] = categoria;
            dt.Rows.Add(r);
        }

        if (dt.Rows.Count > 0)
        {
            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = "Dimension.DimProducto", BatchSize = 1000 };
            bulk.ColumnMappings.Add("Producto_id", "Producto_id");
            bulk.ColumnMappings.Add("Id_Producto_Origen", "Id_Producto_Origen");
            bulk.ColumnMappings.Add("Nombre_producto", "Nombre_producto");
            bulk.ColumnMappings.Add("Categoria", "Categoria");
            await bulk.WriteToServerAsync(dt);
            Console.WriteLine($"Inserted {dt.Rows.Count} productos into Dimension.DimProducto");
        }
        else Console.WriteLine("No new productos to insert.");
    }


    var fuenteFile = Path.Combine(repoCsv, "fuente_datos.csv");
    if (File.Exists(fuenteFile))
    {
        Console.WriteLine("Populating Dimension.DimFuente...");
        var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT Tipo_fuente FROM Dimension.DimFuente";
            using var r = await cmd.ExecuteReaderAsync();
            while (await r.ReadAsync())
            {
                if (r.IsDBNull(0)) { existing.Add(""); continue; }
                var val = r.GetValue(0);
                existing.Add(val?.ToString() ?? "");
            }
        }

        var dt = new DataTable();

        dt.Columns.Add(new DataColumn("Fuente_id", typeof(int)));
        dt.Columns.Add(new DataColumn("Tipo_fuente", typeof(string)));
        dt.Columns.Add(new DataColumn("Plataforma", typeof(string)));
        dt.Columns.Add(new DataColumn("Canal", typeof(string)));

        using var sr = new StreamReader(fuenteFile);
        using var csv = new CsvReader(sr, CultureInfo.InvariantCulture);
        csv.Read(); csv.ReadHeader();
        int nextId = 1;

        foreach (var ex in existing)
        {
            if (int.TryParse(ex, out var n) && n >= nextId) nextId = n + 1;
        }
        while (csv.Read())
        {
            var idRaw = csv.TryGetField("IdFuente", out string? idf) ? idf : null;
            int fuenteId = 0;
            if (!string.IsNullOrWhiteSpace(idRaw))
            {
                var digits = new string(idRaw.Where(char.IsDigit).ToArray());
                if (int.TryParse(digits, out var parsed)) fuenteId = parsed;
            }
            if (fuenteId == 0) fuenteId = nextId++;

            var tipo = csv.GetField("TipoFuente");
            if (string.IsNullOrWhiteSpace(tipo)) continue;
            if (existing.Contains(tipo) || existing.Contains(fuenteId.ToString())) continue;
            var r = dt.NewRow();
            r["Fuente_id"] = fuenteId;
            r["Tipo_fuente"] = tipo;
            r["Plataforma"] = DBNull.Value;
            r["Canal"] = DBNull.Value;
            dt.Rows.Add(r);
        }

        if (dt.Rows.Count > 0)
        {
            using var bulk = new SqlBulkCopy(conn) { DestinationTableName = "Dimension.DimFuente", BatchSize = 500 };
            bulk.ColumnMappings.Add("Fuente_id", "Fuente_id");
            bulk.ColumnMappings.Add("Tipo_fuente", "Tipo_fuente");
            bulk.ColumnMappings.Add("Plataforma", "Plataforma");
            bulk.ColumnMappings.Add("Canal", "Canal");
            await bulk.WriteToServerAsync(dt);
            Console.WriteLine($"Inserted {dt.Rows.Count} fuentes into Dimension.DimFuente");
        }
        else Console.WriteLine("No new fuentes to insert.");
    }


    Console.WriteLine("Ensuring Dimension.DimSentimiento contains Positivo/Neutro/Negativo...");
    var needed = new[] { ("Positivo", 3, 5, "#2ECC71"), ("Neutro", 1, 2, "#F1C40F"), ("Negativo", 0, 0, "#E74C3C") };
    var existingSent = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = "SELECT Clasificacion FROM Dimension.DimSentimiento";
        using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) existingSent.Add(r.IsDBNull(0) ? "" : r.GetString(0));
    }
    var sentDt = new DataTable();

    sentDt.Columns.Add(new DataColumn("Sentimiento_id", typeof(int)));
    sentDt.Columns.Add(new DataColumn("Clasificacion", typeof(string)));
    sentDt.Columns.Add(new DataColumn("Puntuacion_min", typeof(int)));
    sentDt.Columns.Add(new DataColumn("Puntuacion_max", typeof(int)));
    sentDt.Columns.Add(new DataColumn("Color", typeof(string)));

    int nextSentId = 1;

    foreach (var ex in existingSent)
    {
        if (int.TryParse(ex, out var n) && n >= nextSentId) nextSentId = n + 1;
    }
    foreach (var (label, min, max, color) in needed)
    {
        if (existingSent.Contains(label)) continue;
        var r = sentDt.NewRow();
        r["Sentimiento_id"] = nextSentId++;
        r["Clasificacion"] = label;
        r["Puntuacion_min"] = min;
        r["Puntuacion_max"] = max;
        r["Color"] = color;
        sentDt.Rows.Add(r);
    }
    if (sentDt.Rows.Count > 0)
    {
        using var bulk = new SqlBulkCopy(conn) { DestinationTableName = "Dimension.DimSentimiento", BatchSize = 10 };
        bulk.ColumnMappings.Add("Sentimiento_id", "Sentimiento_id");
        bulk.ColumnMappings.Add("Clasificacion", "Clasificacion");
        bulk.ColumnMappings.Add("Puntuacion_min", "Puntuacion_min");
        bulk.ColumnMappings.Add("Puntuacion_max", "Puntuacion_max");
        bulk.ColumnMappings.Add("Color", "Color");
        await bulk.WriteToServerAsync(sentDt);
        Console.WriteLine($"Inserted {sentDt.Rows.Count} sentimiento rows.");
    }
    else Console.WriteLine("No new sentimiento rows required.");


    Console.WriteLine("Populating Dimension.DimTiempo from review CSVs (dates)...");
    var dateSet = new HashSet<DateTime>();
    var reviewFiles = new[] { Path.Combine(repoCsv, "web_reviews.csv"), Path.Combine(repoCsv, "social_comments.csv"), Path.Combine(repoCsv, "surveys_part1.csv") };
    foreach (var f in reviewFiles)
    {
        if (!File.Exists(f)) continue;
        using var sr = new StreamReader(f);
        using var csv = new CsvReader(sr, CultureInfo.InvariantCulture);
        csv.Read(); csv.ReadHeader();
        while (csv.Read())
        {
            if (csv.TryGetField("Fecha", out string? s) && DateTime.TryParse(s, out var d)) dateSet.Add(d.Date);
            else if (csv.TryGetField("created_at", out s) && DateTime.TryParse(s, out d)) dateSet.Add(d.Date);
        }
    }


    var existingDates = new HashSet<DateTime>();
    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = "SELECT Fecha FROM Dimension.DimTiempo";
        using var r = await cmd.ExecuteReaderAsync();
        while (await r.ReadAsync()) if (!r.IsDBNull(0)) existingDates.Add(r.GetDateTime(0).Date);
    }

    var tiempoDt = new DataTable();

    tiempoDt.Columns.Add(new DataColumn("Tiempo_id", typeof(int)));
    tiempoDt.Columns.Add(new DataColumn("Fecha", typeof(DateTime)));
    tiempoDt.Columns.Add(new DataColumn("Dia", typeof(int)));
    tiempoDt.Columns.Add(new DataColumn("Mes", typeof(int)));
    tiempoDt.Columns.Add(new DataColumn("Año", typeof(int))); 
    tiempoDt.Columns.Add(new DataColumn("Trimestre", typeof(int)));
    tiempoDt.Columns.Add(new DataColumn("Nombre_Mes", typeof(string)));
    tiempoDt.Columns.Add(new DataColumn("Nombre_dia", typeof(string)));
    tiempoDt.Columns.Add(new DataColumn("Mes_año", typeof(string)));
    tiempoDt.Columns.Add(new DataColumn("Es_Fin_Semana", typeof(bool)));

    foreach (var d in dateSet.OrderBy(x => x))
    {
        if (existingDates.Contains(d)) continue;
        var r = tiempoDt.NewRow();

        var tiempoId = int.Parse(d.ToString("yyyyMMdd"));
        r["Tiempo_id"] = tiempoId;
        r["Fecha"] = d;
        r["Dia"] = d.Day;
        r["Mes"] = d.Month;
        r["Año"] = d.Year;
        r["Trimestre"] = (d.Month - 1) / 3 + 1;
        r["Nombre_Mes"] = CultureInfo.InvariantCulture.DateTimeFormat.GetMonthName(d.Month);
        r["Nombre_dia"] = d.DayOfWeek.ToString();
        r["Mes_año"] = d.ToString("MM-yyyy");
        r["Es_Fin_Semana"] = (d.DayOfWeek == DayOfWeek.Saturday || d.DayOfWeek == DayOfWeek.Sunday);
        tiempoDt.Rows.Add(r);
    }

    if (tiempoDt.Rows.Count > 0)
    {
        using var bulk = new SqlBulkCopy(conn) { DestinationTableName = "Dimension.DimTiempo", BatchSize = 1000 };
        bulk.ColumnMappings.Add("Tiempo_id", "Tiempo_id");
        bulk.ColumnMappings.Add("Fecha", "Fecha");
        bulk.ColumnMappings.Add("Dia", "Dia");
        bulk.ColumnMappings.Add("Mes", "Mes");
        bulk.ColumnMappings.Add("Año", "Año");
        bulk.ColumnMappings.Add("Trimestre", "Trimestre");
        bulk.ColumnMappings.Add("Nombre_Mes", "Nombre_Mes");
        bulk.ColumnMappings.Add("Nombre_dia", "Nombre_dia");
        bulk.ColumnMappings.Add("Mes_año", "Mes_año");
        bulk.ColumnMappings.Add("Es_Fin_Semana", "Es_Fin_Semana");
        await bulk.WriteToServerAsync(tiempoDt);
        Console.WriteLine($"Inserted {tiempoDt.Rows.Count} fechas into Dimension.DimTiempo");
    }
    else Console.WriteLine("No new fechas to insert into DimTiempo.");

    Console.WriteLine("Fast dimension population complete.");
    return 0;
}
catch (Exception ex)
{
    Console.WriteLine("ERROR: " + ex.Message);
    return 10;
}
