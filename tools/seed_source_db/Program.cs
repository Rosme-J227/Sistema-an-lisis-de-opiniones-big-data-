using System.Data;
using System.Globalization;
using CsvHelper;
using Microsoft.Data.SqlClient;




string? connArg = args.Length > 0 ? args[0] : Environment.GetEnvironmentVariable("ConnectionStrings__SourceOpiniones");
if (string.IsNullOrWhiteSpace(connArg))
{
    Console.WriteLine("ERROR: Provide connection string as first arg or set env var ConnectionStrings__SourceOpiniones");
    return 1;
}

var masterConn = new SqlConnectionStringBuilder(connArg) { InitialCatalog = "master" }.ConnectionString;
var sourceConnBuilder = new SqlConnectionStringBuilder(connArg) { InitialCatalog = "SourceOpiniones" };
var sourceConn = sourceConnBuilder.ConnectionString;

try
{
    Console.WriteLine("1) Creating SourceOpiniones database (drops if exists)...");

    string? createScript = null;
    var dirInfo = new DirectoryInfo(AppContext.BaseDirectory);
    for (int i = 0; i < 8 && dirInfo != null; i++)
    {
        var candidate = Path.Combine(dirInfo.FullName, "sql", "01_create_source_db.sql");
        if (File.Exists(candidate))
        {
            createScript = candidate;
            break;
        }
        dirInfo = dirInfo.Parent;
    }

    if (createScript == null)
    {
        var fallback = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "sql", "01_create_source_db.sql"));
        if (File.Exists(fallback)) createScript = fallback;
    }
    Console.WriteLine($"Using create script path: {createScript ?? "(not found)"}");
    if (createScript == null || !File.Exists(createScript)) throw new FileNotFoundException("Create script not found", createScript ?? "sql\\01_create_source_db.sql");
    var scriptText = File.ReadAllText(createScript);
    using (var conn = new SqlConnection(masterConn))
    {
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandTimeout = 600;

        var batches = System.Text.RegularExpressions.Regex.Split(scriptText, @"^\s*GO\s*$", System.Text.RegularExpressions.RegexOptions.Multiline | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        foreach (var batch in batches)
        {
            var trimmed = batch.Trim();
            if (string.IsNullOrEmpty(trimmed)) continue;
            cmd.CommandText = trimmed;
            await cmd.ExecuteNonQueryAsync();
        }
    }

    Console.WriteLine("2) Seeding Clientes and Productos from csv/clients.csv and csv/products.csv...");

    string repoCsv;
    var d = new DirectoryInfo(AppContext.BaseDirectory);
    string? foundCsv = null;
    for (int i = 0; i < 8 && d != null; i++)
    {
        var candidateDir = Path.Combine(d.FullName, "csv");
        var c1 = Path.Combine(candidateDir, "clients.csv");
        var c2 = Path.Combine(candidateDir, "products.csv");
        if (File.Exists(c1) && File.Exists(c2)) { foundCsv = candidateDir; break; }
        d = d.Parent;
    }
    repoCsv = foundCsv ?? Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "csv"));
    var clientsFile = Path.Combine(repoCsv, "clients.csv");
    var productsFile = Path.Combine(repoCsv, "products.csv");
    if (!File.Exists(clientsFile) || !File.Exists(productsFile)) throw new FileNotFoundException("CSV source files not found in repo csv/ folder.");


    var clientsDt = new DataTable();
    clientsDt.Columns.Add(new DataColumn("Id_Cliente_Origen", typeof(string)));
    clientsDt.Columns.Add(new DataColumn("Nombre", typeof(string)));
    clientsDt.Columns.Add(new DataColumn("Email", typeof(string)));
    using (var reader = new StreamReader(clientsFile))
    using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
    {
        csv.Read(); csv.ReadHeader();
        while (csv.Read())
        {
            var id = csv.GetField("IdCliente");
            var nombre = csv.GetField("Nombre");
            var email = csv.GetField("Email");
            var idOrigen = "C" + int.Parse(id).ToString("000");
            var r = clientsDt.NewRow();
            r["Id_Cliente_Origen"] = idOrigen;
            r["Nombre"] = nombre;
            r["Email"] = email;
            clientsDt.Rows.Add(r);
        }
    }


    var productsDt = new DataTable();
    productsDt.Columns.Add(new DataColumn("Id_Producto_Origen", typeof(string)));
    productsDt.Columns.Add(new DataColumn("Nombre", typeof(string)));
    productsDt.Columns.Add(new DataColumn("Categoria", typeof(string)));
    using (var reader = new StreamReader(productsFile))
    using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
    {
        csv.Read(); csv.ReadHeader();
        while (csv.Read())
        {
            var id = csv.GetField("IdProducto");
            var nombre = csv.GetField("Nombre");
            var categoria = csv.GetField("CategorÃ­a");
            var idOrigen = "P" + int.Parse(id).ToString("000");
            var r = productsDt.NewRow();
            r["Id_Producto_Origen"] = idOrigen;
            r["Nombre"] = nombre;
            r["Categoria"] = categoria;
            productsDt.Rows.Add(r);
        }
    }


    using (var conn = new SqlConnection(sourceConn))
    {
        await conn.OpenAsync();
        using var tran = conn.BeginTransaction();
        try
        {
            using var bulkC = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran)
            {
                DestinationTableName = "dbo.Clientes",
                BatchSize = 1000
            };
            bulkC.ColumnMappings.Add("Id_Cliente_Origen", "Id_Cliente_Origen");
            bulkC.ColumnMappings.Add("Nombre", "Nombre");
            bulkC.ColumnMappings.Add("Email", "Email");
            await bulkC.WriteToServerAsync(clientsDt);

            using var bulkP = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran)
            {
                DestinationTableName = "dbo.Productos",
                BatchSize = 1000
            };
            bulkP.ColumnMappings.Add("Id_Producto_Origen", "Id_Producto_Origen");
            bulkP.ColumnMappings.Add("Nombre", "Nombre");
            bulkP.ColumnMappings.Add("Categoria", "Categoria");
            await bulkP.WriteToServerAsync(productsDt);

            await tran.CommitAsync();
            Console.WriteLine($"Inserted {clientsDt.Rows.Count} clientes and {productsDt.Rows.Count} productos.");
        }
        catch
        {
            await tran.RollbackAsync();
            throw;
        }
    }


    Console.WriteLine("3) Building lookups and inserting Reviews from opinion CSVs...");
    var clienteMap = new Dictionary<string,int>(StringComparer.OrdinalIgnoreCase);
    var productoMap = new Dictionary<string,int>(StringComparer.OrdinalIgnoreCase);
    using (var conn = new SqlConnection(sourceConn))
    {
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT ClienteId, Id_Cliente_Origen FROM dbo.Clientes";
        using var rdr = await cmd.ExecuteReaderAsync();
        while (await rdr.ReadAsync()) clienteMap[rdr.GetString(1)] = rdr.GetInt32(0);
        rdr.Close();
        cmd.CommandText = "SELECT ProductoId, Id_Producto_Origen FROM dbo.Productos";
        using var rdr2 = await cmd.ExecuteReaderAsync();
        while (await rdr2.ReadAsync()) productoMap[rdr2.GetString(1)] = rdr2.GetInt32(0);
    }

    var reviewFiles = new[] { ("web_reviews.csv","Web"), ("social_comments.csv","Social"), ("surveys_part1.csv","Survey") };
    var reviewsDt = new DataTable();
    reviewsDt.Columns.Add(new DataColumn("IdOrigen", typeof(string)));
    reviewsDt.Columns.Add(new DataColumn("ClienteId", typeof(int)));
    reviewsDt.Columns.Add(new DataColumn("ProductoId", typeof(int)));
    reviewsDt.Columns.Add(new DataColumn("Fuente", typeof(string)));
    reviewsDt.Columns.Add(new DataColumn("Fecha", typeof(DateTime)));
    reviewsDt.Columns.Add(new DataColumn("Comentario", typeof(string)));
    reviewsDt.Columns.Add(new DataColumn("Puntaje", typeof(int)));
    reviewsDt.Columns.Add(new DataColumn("Rating", typeof(int)));

    foreach (var (fileName, fuente) in reviewFiles)
    {
        var path = Path.Combine(repoCsv, fileName);
        if (!File.Exists(path)) { Console.WriteLine($"  Skipping missing {fileName}"); continue; }
        using var reader = new StreamReader(path);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        csv.Read(); csv.ReadHeader();
        while (csv.Read())
        {

            csv.TryGetField("IdReview", out string? idReview);
            if (string.IsNullOrEmpty(idReview)) csv.TryGetField("IdComment", out idReview);
            csv.TryGetField("IdCliente", out string? idClienteRaw);
            if (string.IsNullOrEmpty(idClienteRaw)) csv.TryGetField("IdClient", out idClienteRaw);
            csv.TryGetField("IdProducto", out string? idProductoRaw);
            if (string.IsNullOrEmpty(idProductoRaw)) csv.TryGetField("IdProduct", out idProductoRaw);
            csv.TryGetField("Fecha", out string? fechaS);
            if (string.IsNullOrEmpty(fechaS)) csv.TryGetField("Fec", out fechaS);
            var comentario = csv.TryGetField("Comentario", out string? c) ? c : csv.TryGetField("comment", out c) ? c : null;
            var ratingS = csv.TryGetField("Rating", out string? r) ? r : null;
            var puntajeS = csv.TryGetField("Puntaje", out string? p) ? p : null;


            var clienteKey = idClienteRaw?.Trim();
            var productoKey = idProductoRaw?.Trim();
            if (!string.IsNullOrEmpty(clienteKey) && clienteKey.StartsWith("C", StringComparison.OrdinalIgnoreCase)) clienteKey = clienteKey.ToUpper();
            if (!string.IsNullOrEmpty(productoKey) && productoKey.StartsWith("P", StringComparison.OrdinalIgnoreCase)) productoKey = productoKey.ToUpper();

            if (!clienteMap.TryGetValue(clienteKey ?? "", out var clienteId)) clienteId = 0;
            if (!productoMap.TryGetValue(productoKey ?? "", out var productoId)) productoId = 0;

            if (clienteId == 0 || productoId == 0)
            {

                if (clienteId == 0 && clienteKey != null && clienteKey.Length>1 && int.TryParse(clienteKey[1..], out var n)) clienteMap.TryGetValue("C"+n.ToString("000"), out clienteId);
                if (productoId == 0 && productoKey != null && productoKey.Length>1 && int.TryParse(productoKey[1..], out var m)) productoMap.TryGetValue("P"+m.ToString("000"), out productoId);
            }

            var row = reviewsDt.NewRow();
            row["IdOrigen"] = idReview ?? (Guid.NewGuid().ToString());
            if (clienteId != 0) row["ClienteId"] = clienteId; else row["ClienteId"] = DBNull.Value;
            if (productoId != 0) row["ProductoId"] = productoId; else row["ProductoId"] = DBNull.Value;
            row["Fuente"] = fuente;
            if (DateTime.TryParse(fechaS, out var dt)) row["Fecha"] = dt; else row["Fecha"] = DateTime.UtcNow;
            row["Comentario"] = comentario ?? string.Empty;
            if (int.TryParse(puntajeS, out var pt)) row["Puntaje"] = pt; else row["Puntaje"] = DBNull.Value;
            if (int.TryParse(ratingS, out var rt)) row["Rating"] = rt; else row["Rating"] = DBNull.Value;
            reviewsDt.Rows.Add(row);
        }
    }

    using (var conn = new SqlConnection(sourceConn))
    {
        await conn.OpenAsync();
        using var tran = conn.BeginTransaction();
        try
        {
            using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran) { DestinationTableName = "dbo.Reviews", BatchSize = 2000 };
            bulk.ColumnMappings.Add("IdOrigen", "IdOrigen");
            bulk.ColumnMappings.Add("ClienteId", "ClienteId");
            bulk.ColumnMappings.Add("ProductoId", "ProductoId");
            bulk.ColumnMappings.Add("Fuente", "Fuente");
            bulk.ColumnMappings.Add("Fecha", "Fecha");
            bulk.ColumnMappings.Add("Comentario", "Comentario");
            bulk.ColumnMappings.Add("Puntaje", "Puntaje");
            bulk.ColumnMappings.Add("Rating", "Rating");
            await bulk.WriteToServerAsync(reviewsDt);
            await tran.CommitAsync();
            Console.WriteLine($"Inserted {reviewsDt.Rows.Count} reviews into SourceOpiniones.dbo.Reviews");
        }
        catch
        {
            await tran.RollbackAsync();
            throw;
        }
    }

    Console.WriteLine("Seeding complete.");
    return 0;
}
catch (Exception ex)
{
    Console.WriteLine("ERROR: " + ex.Message);
    return 2;
}
