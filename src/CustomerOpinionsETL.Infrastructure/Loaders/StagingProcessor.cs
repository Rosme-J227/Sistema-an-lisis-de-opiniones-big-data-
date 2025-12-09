using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using CustomerOpinionsETL.Core.Entities;
using CustomerOpinionsETL.Core.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace CustomerOpinionsETL.Infrastructure.Loaders;

public class StagingProcessor : IStagingProcessor
{
    private readonly string _dwhConnection;
    private readonly ISentimentAnalyzer _sentiment;
    private readonly ILogService _log;
    private readonly ILogger<StagingProcessor>? _logger;
    private const int DuplicateWindowDays = 7;

    public StagingProcessor(
        string dwhConnection,
        ISentimentAnalyzer sentiment,
        ILogService log,
        ILogger<StagingProcessor>? logger = null)
    {
        _dwhConnection = dwhConnection;
        _sentiment = sentiment;
        _log = log;
        _logger = logger;
    }

    public async Task ProcessNewAsync(CancellationToken ct = default)
    {
        var sw = Stopwatch.StartNew();
        _logger?.LogInformation("Starting staging processor");

        await using var con = new SqlConnection(_dwhConnection);
        await con.OpenAsync(ct);

        int processed = 0;
        int duplicates = 0;
        int errors = 0;

        try
        {
            using var transaction = con.BeginTransaction(IsolationLevel.ReadCommitted);


            var cmdRead = con.CreateCommand();
            cmdRead.Transaction = transaction;
            cmdRead.CommandText = @"SELECT StgId, Fuente, IdOrigen, ClienteId, NombreCliente, Email, CodigoProducto, 
                                           Comentario, Puntaje, Rating, FechaOrigen 
                                    FROM Staging.StgOpiniones 
                                    WHERE EstadoCarga='NEW' 
                                    ORDER BY StgId";

            await using var reader = await cmdRead.ExecuteReaderAsync(ct);
            var rows = new List<StagingRow>();

            while (await reader.ReadAsync(ct))
            {
                rows.Add(new StagingRow
                {
                    StgId = reader.GetInt64(0),
                    Fuente = reader.IsDBNull(1) ? null : reader.GetString(1),
                    IdOrigen = reader.IsDBNull(2) ? null : reader.GetString(2),
                    ClienteId = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3),
                    NombreCliente = reader.IsDBNull(4) ? null : reader.GetString(4),
                    Email = reader.IsDBNull(5) ? null : reader.GetString(5),
                    CodigoProducto = reader.IsDBNull(6) ? null : reader.GetString(6),
                    Comentario = reader.IsDBNull(7) ? null : reader.GetString(7),
                    Puntaje = reader.IsDBNull(8) ? (int?)null : reader.GetInt32(8),
                    Rating = reader.IsDBNull(9) ? (int?)null : reader.GetInt32(9),
                    FechaOrigen = reader.IsDBNull(10) ? DateTime.UtcNow : reader.GetDateTime(10)
                });
            }
            reader.Close();

            _logger?.LogInformation("Read {RowCount} new staging rows", rows.Count);


            var existingHashes = await LoadExistingHashesAsync(con, transaction, ct);

            foreach (var row in rows)
            {
                if (ct.IsCancellationRequested) break;

                try
                {

                    row.Comentario = CleanComment(row.Comentario);
                    row.NombreCliente = NormalizeName(row.NombreCliente);
                    row.Email = row.Email?.Trim().ToLowerInvariant();

                    if (!ValidateRow(row))
                    {
                        _logger?.LogDebug("Row {StgId} validation failed", row.StgId);
                        await MarkRowAsync(con, transaction, row.StgId, "VALIDATION_ERROR", ct);
                        errors++;
                        continue;
                    }


                    string commentHash = ComputeHash(row.Comentario);
                    if (existingHashes.Contains(commentHash))
                    {
                        _logger?.LogDebug("Row {StgId} is duplicate (hash match)", row.StgId);
                        await MarkRowAsync(con, transaction, row.StgId, "DUPLICATE", ct);
                        duplicates++;
                        continue;
                    }


                    int clienteId = await EnsureClienteAsync(con, transaction, row, ct);
                    int productoId = await EnsureProductoAsync(con, transaction, row, ct);
                    int fuenteId = await EnsureFuenteAsync(con, transaction, row, ct);


                    var (clasificacion, score) = _sentiment.Analyze(row.Comentario);
                    int sentimientoId = await EnsureSentimientoAsync(con, transaction, clasificacion, ct);


                    int tiempoId = await EnsureTiempoAsync(con, transaction, row.FechaOrigen, ct);


                    long opinionId = await GenerateOpinionIdAsync(con, transaction, ct);


                    await InsertFactOpinionAsync(con, transaction, opinionId, tiempoId, productoId, 
                        clienteId, fuenteId, sentimientoId, row, score, commentHash, ct);


                    await MarkRowAsync(con, transaction, row.StgId, "PROCESSED", ct);

                    existingHashes.Add(commentHash);
                    processed++;

                    _logger?.LogDebug("Successfully processed row {StgId} (Opinion_id: {OpinionId})", row.StgId, opinionId);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error processing row {StgId}: {Message}", row.StgId, ex.Message);
                    await MarkRowAsync(con, transaction, row.StgId, "ERROR", ct);
                    errors++;
                }
            }

            await transaction.CommitAsync(ct);
            sw.Stop();

            _logger?.LogInformation(
                "Staging processor completed in {ElapsedMs}ms. Processed: {Processed}, Duplicates: {Duplicates}, Errors: {Errors}",
                sw.ElapsedMilliseconds, processed, duplicates, errors);

            _log.Info($"Processed: {processed}, Duplicates: {duplicates}, Errors: {errors}");
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Staging processor failed: {Message}", ex.Message);
            _log.Error("Staging processor failed", ex);
            throw;
        }
    }

    private async Task<HashSet<string>> LoadExistingHashesAsync(SqlConnection con, SqlTransaction transaction, CancellationToken ct)
    {
        var hashes = new HashSet<string>();
        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        // Check that the column exists before querying to avoid SQL errors on databases that haven't been migrated
        cmd.CommandText = @"SELECT COL_LENGTH('Fact.FactOpiniones', 'Hash_Comentario')";
        var colLen = await cmd.ExecuteScalarAsync(ct);
        if (colLen == null || colLen == DBNull.Value)
        {
            _logger?.LogInformation("Hash_Comentario column not present in Fact.FactOpiniones - skipping duplicate hash load");
            return hashes;
        }

        cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = @"SELECT DISTINCT Hash_Comentario 
                           FROM Fact.FactOpiniones 
                           WHERE Fecha_carga > DATEADD(DAY, -@Days, GETDATE())
                           AND Hash_Comentario IS NOT NULL";
        cmd.Parameters.AddWithValue("@Days", DuplicateWindowDays);

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            hashes.Add(reader.GetString(0));
        }

        _logger?.LogInformation("Loaded {HashCount} existing hashes for duplicate detection", hashes.Count);
        return hashes;
    }

    private string ComputeHash(string text)
    {
        if (string.IsNullOrEmpty(text)) return "";
        using var md5 = MD5.Create();
        var hash = md5.ComputeHash(Encoding.UTF8.GetBytes(text.ToLowerInvariant()));
        return Convert.ToBase64String(hash);
    }

    private bool ValidateRow(StagingRow row)
    {

        if (string.IsNullOrWhiteSpace(row.Comentario) || row.Comentario.Length < 3)
            return false;


        if (row.Puntaje.HasValue && (row.Puntaje < 0 || row.Puntaje > 100))
            return false;


        if (row.Rating.HasValue && (row.Rating < 1 || row.Rating > 5))
            return false;


        if (row.FechaOrigen < new DateTime(2020, 1, 1) || row.FechaOrigen > DateTime.UtcNow)
            return false;


        if (string.IsNullOrWhiteSpace(row.IdOrigen) && !row.ClienteId.HasValue && 
            string.IsNullOrWhiteSpace(row.Email) && string.IsNullOrWhiteSpace(row.NombreCliente))
            return false;

        return true;
    }

    private string CleanComment(string? text)
    {
        if (string.IsNullOrEmpty(text)) return "";


        text = Regex.Replace(text, @"<[^>]*>", "");


        text = Regex.Replace(text, @"[\x00-\x1F\x7F-\x9F]", " ");


        text = Regex.Replace(text, "[^\\w\\s\\.\\,\\;\\:\\!\\?\\'\\\"\\-]", "");


        text = Regex.Replace(text, @"\s+", " ");

        return text.Trim();
    }

    private string NormalizeName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name)) return "";
        name = name.Trim();

        var words = name.Split(' ');
        var normalized = string.Join(" ", words.Select(w => 
            char.ToUpper(w[0]) + (w.Length > 1 ? w.Substring(1).ToLower() : "")));
        return normalized;
    }

    private async Task<int> EnsureClienteAsync(SqlConnection con, SqlTransaction transaction, StagingRow row, CancellationToken ct)
    {

        if (!string.IsNullOrWhiteSpace(row.Email))
        {
            var cmd = con.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "SELECT Cliente_id FROM Dimension.DimCliente WHERE Email = @Email";
            cmd.Parameters.AddWithValue("@Email", row.Email);
            var obj = await cmd.ExecuteScalarAsync(ct);
            if (obj != null && obj != DBNull.Value) return Convert.ToInt32(obj);
        }


        if (row.ClienteId.HasValue)
        {
            var cmd = con.CreateCommand();
            cmd.Transaction = transaction;
            cmd.CommandText = "SELECT Cliente_id FROM Dimension.DimCliente WHERE Id_Cliente_origen = @IdOrigen";
            cmd.Parameters.AddWithValue("@IdOrigen", row.ClienteId.Value.ToString());
            var obj = await cmd.ExecuteScalarAsync(ct);
            if (obj != null && obj != DBNull.Value) return Convert.ToInt32(obj);
        }


        var ins = con.CreateCommand();
        ins.Transaction = transaction;
        ins.CommandText = @"INSERT INTO Dimension.DimCliente (Id_Cliente_origen, Nombre_Cliente, Email) 
                           OUTPUT INSERTED.Cliente_id 
                           VALUES (@IdOrigen, @Nombre, @Email)";
        ins.Parameters.AddWithValue("@IdOrigen", row.ClienteId?.ToString() ?? (object)DBNull.Value);
        ins.Parameters.AddWithValue("@Nombre", row.NombreCliente ?? (object)DBNull.Value);
        ins.Parameters.AddWithValue("@Email", row.Email ?? (object)DBNull.Value);
        var newId = await ins.ExecuteScalarAsync(ct);
        return Convert.ToInt32(newId);
    }

    private async Task<int> EnsureProductoAsync(SqlConnection con, SqlTransaction transaction, StagingRow row, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(row.CodigoProducto))
            return await GetDefaultProductoIdAsync(con, transaction, ct);

        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "SELECT Producto_id FROM Dimension.DimProducto WHERE Id_Producto_Origen = @IdOrigen";
        cmd.Parameters.AddWithValue("@IdOrigen", row.CodigoProducto);
        var obj = await cmd.ExecuteScalarAsync(ct);
        if (obj != null && obj != DBNull.Value) return Convert.ToInt32(obj);

        var ins = con.CreateCommand();
        ins.Transaction = transaction;
        ins.CommandText = @"INSERT INTO Dimension.DimProducto (Id_Producto_Origen, Nombre_producto, Categoria, Fecha_alta, activo) 
                           OUTPUT INSERTED.Producto_id 
                           VALUES (@IdOrigen, @Nombre, @Categoria, @FechaAlta, @Activo)";
        ins.Parameters.AddWithValue("@IdOrigen", row.CodigoProducto);
        ins.Parameters.AddWithValue("@Nombre", row.CodigoProducto);
        ins.Parameters.AddWithValue("@Categoria", "Desconocida");
        ins.Parameters.AddWithValue("@FechaAlta", DateTime.UtcNow.Date);
        ins.Parameters.AddWithValue("@Activo", 1);
        var newId = await ins.ExecuteScalarAsync(ct);
        return Convert.ToInt32(newId);
    }

    private async Task<int> GetDefaultProductoIdAsync(SqlConnection con, SqlTransaction transaction, CancellationToken ct)
    {
        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "SELECT TOP 1 Producto_id FROM Dimension.DimProducto ORDER BY Producto_id";
        var obj = await cmd.ExecuteScalarAsync(ct);
        return obj != null ? Convert.ToInt32(obj) : 1;
    }

    private async Task<int> EnsureSentimientoAsync(SqlConnection con, SqlTransaction transaction, string clasificacion, CancellationToken ct)
    {
        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "SELECT Sentimiento_id FROM Dimension.DimSentimiento WHERE Clasificacion = @Clasificacion";
        cmd.Parameters.AddWithValue("@Clasificacion", clasificacion);
        var obj = await cmd.ExecuteScalarAsync(ct);
        if (obj != null && obj != DBNull.Value) return Convert.ToInt32(obj);

        var ins = con.CreateCommand();
        ins.Transaction = transaction;
        ins.CommandText = @"INSERT INTO Dimension.DimSentimiento (Clasificacion, Puntuacion_min, Puntuacion_max, Color) 
                           OUTPUT INSERTED.Sentimiento_id 
                           VALUES (@Clasificacion, 0, 100, @Color)";
        ins.Parameters.AddWithValue("@Clasificacion", clasificacion);
        ins.Parameters.AddWithValue("@Color", clasificacion == "Positive" ? "Green" : 
                                              clasificacion == "Negative" ? "Red" : "Gray");
        var newId = await ins.ExecuteScalarAsync(ct);
        return Convert.ToInt32(newId);
    }

    private async Task<int> EnsureTiempoAsync(SqlConnection con, SqlTransaction transaction, DateTime fecha, CancellationToken ct)
    {
        var dateOnly = fecha.Date;
        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "SELECT Tiempo_id FROM Dimension.DimTiempo WHERE Fecha = @Fecha";
        cmd.Parameters.AddWithValue("@Fecha", dateOnly);
        var obj = await cmd.ExecuteScalarAsync(ct);
        if (obj != null && obj != DBNull.Value) return Convert.ToInt32(obj);

        var ins = con.CreateCommand();
        ins.Transaction = transaction;
        ins.CommandText = @"INSERT INTO Dimension.DimTiempo 
                   (Fecha, Dia, Mes, [Año], Trimestre, Nombre_Mes, Nombre_dia, Mes_año, Es_Fin_Semana)
                   OUTPUT INSERTED.Tiempo_id
                   VALUES (@Fecha, @Dia, @Mes, @Ano, @Trimestre, @NombreMes, @NombreDia, @MesAno, @EsFin)";
        ins.Parameters.AddWithValue("@Fecha", dateOnly);
        ins.Parameters.AddWithValue("@Dia", fecha.Day);
        ins.Parameters.AddWithValue("@Mes", fecha.Month);
        ins.Parameters.AddWithValue("@Ano", fecha.Year);
        ins.Parameters.AddWithValue("@Trimestre", ((fecha.Month - 1) / 3) + 1);
        ins.Parameters.AddWithValue("@NombreMes", fecha.ToString("MMMM"));
        ins.Parameters.AddWithValue("@NombreDia", fecha.ToString("dddd"));
        ins.Parameters.AddWithValue("@MesAno", $"{fecha.Month}/{fecha.Year}");
        ins.Parameters.AddWithValue("@EsFin", (fecha.DayOfWeek == DayOfWeek.Saturday || fecha.DayOfWeek == DayOfWeek.Sunday) ? 1 : 0);
        var newId = await ins.ExecuteScalarAsync(ct);
        return Convert.ToInt32(newId);
    }

    private async Task<int> EnsureFuenteAsync(SqlConnection con, SqlTransaction transaction, StagingRow row, CancellationToken ct)
    {
        string fuente = row.Fuente ?? "UNKNOWN";

        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "SELECT Fuente_id FROM Dimension.DimFuente WHERE Tipo_fuente = @Tipo";
        cmd.Parameters.AddWithValue("@Tipo", fuente);
        var obj = await cmd.ExecuteScalarAsync(ct);
        if (obj != null && obj != DBNull.Value) return Convert.ToInt32(obj);


        var cmdMax = con.CreateCommand();
        cmdMax.Transaction = transaction;
        cmdMax.CommandText = "SELECT ISNULL(MAX(Fuente_id), 0) + 1 FROM Dimension.DimFuente";
        var nextId = Convert.ToInt32(await cmdMax.ExecuteScalarAsync(ct));

        var ins = con.CreateCommand();
        ins.Transaction = transaction;
        ins.CommandText = @"INSERT INTO Dimension.DimFuente (Fuente_id, Tipo_fuente, Plataforma, Canal) 
                           VALUES (@Id, @Tipo, @Plataforma, @Canal)";
        ins.Parameters.AddWithValue("@Id", nextId);
        ins.Parameters.AddWithValue("@Tipo", fuente);
        ins.Parameters.AddWithValue("@Plataforma", DBNull.Value);
        ins.Parameters.AddWithValue("@Canal", DBNull.Value);
        await ins.ExecuteNonQueryAsync(ct);
        return nextId;
    }

    private async Task<long> GenerateOpinionIdAsync(SqlConnection con, SqlTransaction transaction, CancellationToken ct)
    {
        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "SELECT ISNULL(MAX(Opinion_id), 0) + 1 FROM Fact.FactOpiniones";
        var obj = await cmd.ExecuteScalarAsync(ct);
        return Convert.ToInt64(obj ?? 1);
    }

    private async Task InsertFactOpinionAsync(SqlConnection con, SqlTransaction transaction, long opinionId, 
        int tiempoId, int productoId, int clienteId, int fuenteId, int sentimientoId, StagingRow row, 
        double score, string hash, CancellationToken ct)
    {
        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = @"INSERT INTO Fact.FactOpiniones
            (Opinion_id, Tiempo_id, Producto_id, Cliente_id, Fuente_id, Sentimiento_id, ID_Opinion_Original, 
             Comentario, Puntaje_Satisfaccion, Rating, Longitud_Comentario, Tiene_Comentario, 
             Palabras_clavePositivas, Palabras_claveNegativas, Score_sentimiento, Hash_Comentario, Fecha_carga)
            VALUES (@OpinionId, @TiempoId, @ProductoId, @ClienteId, @FuenteId, @SentimientoId, @IdOrigen, 
                    @Comentario, @Puntaje, @Rating, @Longitud, @TieneComentario, @PalPos, @PalNeg, @Score, @Hash, GETDATE())";

        cmd.Parameters.AddWithValue("@OpinionId", opinionId);
        cmd.Parameters.AddWithValue("@TiempoId", tiempoId);
        cmd.Parameters.AddWithValue("@ProductoId", productoId);
        cmd.Parameters.AddWithValue("@ClienteId", clienteId);
        cmd.Parameters.AddWithValue("@FuenteId", fuenteId);
        cmd.Parameters.AddWithValue("@SentimientoId", sentimientoId);
        cmd.Parameters.AddWithValue("@IdOrigen", row.IdOrigen ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Comentario", row.Comentario ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Puntaje", row.Puntaje ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Rating", row.Rating ?? (object)DBNull.Value);
        cmd.Parameters.AddWithValue("@Longitud", row.Comentario?.Length ?? 0);
        cmd.Parameters.AddWithValue("@TieneComentario", string.IsNullOrWhiteSpace(row.Comentario) ? 0 : 1);
        cmd.Parameters.AddWithValue("@PalPos", DBNull.Value);
        cmd.Parameters.AddWithValue("@PalNeg", DBNull.Value);
        cmd.Parameters.AddWithValue("@Score", score);
        cmd.Parameters.AddWithValue("@Hash", hash);

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task MarkRowAsync(SqlConnection con, SqlTransaction transaction, long stgId, string status, CancellationToken ct)
    {
        var cmd = con.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = "UPDATE Staging.StgOpiniones SET EstadoCarga = @Status, FechaActualizacion = GETDATE() WHERE StgId = @StgId";
        cmd.Parameters.AddWithValue("@Status", status);
        cmd.Parameters.AddWithValue("@StgId", stgId);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private class StagingRow
    {
        public long StgId { get; set; }
        public string? Fuente { get; set; }
        public string? IdOrigen { get; set; }
        public int? ClienteId { get; set; }
        public string? NombreCliente { get; set; }
        public string? Email { get; set; }
        public string? CodigoProducto { get; set; }
        public string? Comentario { get; set; }
        public int? Puntaje { get; set; }
        public int? Rating { get; set; }
        public DateTime FechaOrigen { get; set; }
    }
}
