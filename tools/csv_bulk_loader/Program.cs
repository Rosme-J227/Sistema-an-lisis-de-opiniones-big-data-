using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using System.Linq;
using Microsoft.Data.SqlClient;




string? connFromArg = args.Length > 0 ? args[0] : null;
string? csvFolder = args.Length > 1 ? args[1] : Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "csv");

var connectionString = connFromArg ?? Environment.GetEnvironmentVariable("ConnectionStrings__DWOpinionClientes");
if (string.IsNullOrWhiteSpace(connectionString))
{
    Console.WriteLine("ERROR: Connection string not provided. Set env var ConnectionStrings__DWOpinionClientes or pass as first argument.");
    return 1;
}

if (!Directory.Exists(csvFolder))
{
    Console.WriteLine($"ERROR: CSV folder not found: {csvFolder}");
    return 1;
}

var csvFiles = Directory.GetFiles(csvFolder, "*.csv");
if (csvFiles.Length == 0)
{
    Console.WriteLine($"No CSV files found in {csvFolder}");
    return 0;
}

Console.WriteLine($"Found {csvFiles.Length} CSV files. Loading to Staging.StgOpiniones using connection: {connectionString.Split(';')[0]}...");

foreach (var file in csvFiles)
{
    Console.WriteLine($"Processing {Path.GetFileName(file)}...");
    try
    {
        var table = await ReadCsvToDataTableAsync(file);
        if (table.Rows.Count == 0)
        {
            Console.WriteLine("  No rows found, skipping.");
            continue;
        }

        using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();

        using var tran = conn.BeginTransaction();
        using var bulk = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, tran)
        {
            DestinationTableName = "Staging.StgOpiniones",
            BatchSize = 5000,
            BulkCopyTimeout = 600
        };


        foreach (DataColumn col in table.Columns)
        {
            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);
        }

        await bulk.WriteToServerAsync(table);
        await tran.CommitAsync();
        Console.WriteLine($"  Inserted {table.Rows.Count} rows from {Path.GetFileName(file)}.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"  ERROR processing {Path.GetFileName(file)}: {ex.Message}");
    }
}

Console.WriteLine("Done.");
return 0;

async Task<DataTable> ReadCsvToDataTableAsync(string path)
{
    var dt = CreateStagingDataTable();

    using var reader = new StreamReader(path);

    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

    await csv.ReadAsync();
    csv.ReadHeader();

    var headerObj = csv.Context.GetType().GetProperty("HeaderRecord")?.GetValue(csv.Context);
    var headerArray = headerObj as string[] ?? Array.Empty<string>();
    var headers = headerArray.Select(h => h.Trim()).ToArray();

    while (await csv.ReadAsync())
    {
        var row = dt.NewRow();

        MapIfExists(csv, headers, "Fuente", v => row["Fuente"] = v);
        MapIfExists(csv, headers, "IdOrigen", v => row["IdOrigen"] = v);
        MapIfExists(csv, headers, "ClienteId", v => row["ClienteId"] = ParseInt(v));
        MapIfExists(csv, headers, "NombreCliente", v => row["NombreCliente"] = v);
        MapIfExists(csv, headers, "Email", v => row["Email"] = v);
        MapIfExists(csv, headers, "CodigoProducto", v => row["CodigoProducto"] = v);
        MapIfExists(csv, headers, "Comentario", v => row["Comentario"] = v);
        MapIfExists(csv, headers, "Puntaje", v => row["Puntaje"] = ParseInt(v));
        MapIfExists(csv, headers, "Rating", v => row["Rating"] = ParseInt(v));
        MapIfExists(csv, headers, "FechaOrigen", v => row["FechaOrigen"] = ParseDate(v));


        MapIfExists(csv, headers, "comment", v => { if (string.IsNullOrWhiteSpace(row["Comentario"].ToString())) row["Comentario"] = v; });
        MapIfExists(csv, headers, "created_at", v => { if (row.IsNull("FechaOrigen")) row["FechaOrigen"] = ParseDate(v); });


        row["EstadoCarga"] = "NEW";
        dt.Rows.Add(row);
    }

    return dt;

    static void MapIfExists(CsvReader csv, string[] headers, string headerName, Action<string?> assign)
    {
        var idx = Array.FindIndex(headers, h => string.Equals(h, headerName, StringComparison.OrdinalIgnoreCase));
        if (idx >= 0)
        {
            var val = csv.GetField(idx);
            assign(string.IsNullOrWhiteSpace(val) ? null : val.Trim());
        }
    }

    static object? ParseInt(string? s)
    {
        if (int.TryParse(s, out var v)) return v;
        return DBNull.Value;
    }

    static object? ParseDate(string? s)
    {
        if (string.IsNullOrWhiteSpace(s)) return DBNull.Value;
        if (DateTime.TryParse(s, out var d)) return d;
        if (DateTime.TryParseExact(s, new[] { "yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy", "yyyy-MM-ddTHH:mm:ss" }, CultureInfo.InvariantCulture, DateTimeStyles.None, out d)) return d;
        return DBNull.Value;
    }
}

DataTable CreateStagingDataTable()
{
    var dt = new DataTable();
    dt.Columns.Add(new DataColumn("Fuente", typeof(string)));
    dt.Columns.Add(new DataColumn("IdOrigen", typeof(string)));
    dt.Columns.Add(new DataColumn("ClienteId", typeof(int)));
    dt.Columns.Add(new DataColumn("NombreCliente", typeof(string)));
    dt.Columns.Add(new DataColumn("Email", typeof(string)));
    dt.Columns.Add(new DataColumn("CodigoProducto", typeof(string)));
    dt.Columns.Add(new DataColumn("Comentario", typeof(string)));
    dt.Columns.Add(new DataColumn("Puntaje", typeof(int)));
    dt.Columns.Add(new DataColumn("Rating", typeof(int)));
    dt.Columns.Add(new DataColumn("FechaOrigen", typeof(DateTime)));
    dt.Columns.Add(new DataColumn("EstadoCarga", typeof(string)));
    dt.Columns.Add(new DataColumn("FechaActualizacion", typeof(DateTime)));
    return dt;
}
