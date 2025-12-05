using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CustomerOpinionsETL.Core.Entities;
using CustomerOpinionsETL.Core.Interfaces;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace CustomerOpinionsETL.Infrastructure.Extractors;

public class DatabaseExtractor : IExtractor
{
    private readonly string _connectionString;
    private readonly string _query;
    private readonly int _commandTimeout;
    private readonly int _maxRetries;
    private readonly ILogger<DatabaseExtractor>? _logger;
    private readonly IConfiguration? _config;

    public DatabaseExtractor(
        string connectionString,
        string? query = null,
        int commandTimeout = 300,
        int maxRetries = 3,
        ILogger<DatabaseExtractor>? logger = null,
        IConfiguration? config = null)
    {
        _connectionString = connectionString;
        _query = query ?? config?["Database:Query"] ?? "SELECT IdOrigen, ClienteId, ProductoId, Fuente, Fecha, Comentario, Puntaje, Rating FROM SourceOpiniones.dbo.Reviews ORDER BY ReviewId";
        _commandTimeout = config?.GetValue<int>("Database:Timeout") ?? commandTimeout;
        _maxRetries = maxRetries;
        _logger = logger;
        _config = config;
    }

    public async Task<List<OpinionRaw>> ExtractAsync(System.Threading.CancellationToken ct = default)
    {
        var results = new List<OpinionRaw>();
        _logger?.LogInformation("Starting database extraction");


        if (!await ValidateConnectionAsync(ct))
        {
            _logger?.LogError("Database connection validation failed");
            return results;
        }

        int rowsProcessed = 0;
        int rowsError = 0;

        try
        {

            var opinions = await ExecuteWithRetryAsync(ct);
            foreach (var opinion in opinions)
            {
                if (ValidateOpinion(opinion))
                {
                    rowsProcessed++;
                    results.Add(opinion);
                }
                else
                {
                    rowsError++;
                }
            }

            _logger?.LogInformation("Database extraction completed. Processed: {Processed}, Errors: {Errors}",
                rowsProcessed, rowsError);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Database extraction failed: {Message}", ex.Message);
            throw;
        }

        return results;
    }

    private async Task<bool> ValidateConnectionAsync(CancellationToken ct)
    {
        try
        {
            await using var con = new SqlConnection(_connectionString);
            await con.OpenAsync(ct);
            _logger?.LogInformation("Database connection validated successfully");
            return true;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Database connection validation failed: {Message}", ex.Message);
            return false;
        }
    }

    private async Task<List<OpinionRaw>> ExecuteWithRetryAsync(CancellationToken ct)
    {
        int attempt = 0;
        while (attempt < _maxRetries)
        {
            try
            {
                attempt++;
                _logger?.LogInformation("Database query attempt {Attempt} of {MaxRetries}", attempt, _maxRetries);

                var opinions = new List<OpinionRaw>();

                await using var con = new SqlConnection(_connectionString);
                await con.OpenAsync(ct);

                await using var cmd = con.CreateCommand();
                cmd.CommandText = _query;
                cmd.CommandTimeout = _commandTimeout;

                await using var reader = await cmd.ExecuteReaderAsync(ct);

                while (await reader.ReadAsync(ct))
                {
                    try
                    {
                        var opinion = new OpinionRaw
                        {
                            Fuente = reader["Fuente"]?.ToString() ?? "DATABASE",
                            IdOrigen = reader["IdOrigen"]?.ToString(),
                            ClienteId = reader["ClienteId"] as int?,
                            CodigoProducto = reader["ProductoId"]?.ToString(),
                            Comentario = reader["Comentario"]?.ToString(),
                            Puntaje = reader["Puntaje"] as int?,
                            Rating = reader["Rating"] as int?,
                            FechaOrigen = reader["Fecha"] as System.DateTime? ?? System.DateTime.UtcNow
                        };

                        opinions.Add(opinion);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogWarning(ex, "Error mapping row from database");
                    }
                }

                _logger?.LogInformation("Database query succeeded on attempt {Attempt}. Rows: {RowCount}", attempt, opinions.Count);
                return opinions;
            }
            catch (SqlException ex) when (attempt < _maxRetries && IsTransientError(ex))
            {
                int delayMs = (int)Math.Pow(2, attempt) * 1000;
                _logger?.LogWarning("Transient database error on attempt {Attempt}. Retrying in {DelayMs}ms: {Message}",
                    attempt, delayMs, ex.Message);
                await Task.Delay(delayMs, ct);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Database query failed on attempt {Attempt}: {Message}", attempt, ex.Message);
                if (attempt >= _maxRetries) throw;
                await Task.Delay(1000 * attempt, ct);
            }
        }

        throw new InvalidOperationException($"Database extraction failed after {_maxRetries} attempts");
    }

    private bool IsTransientError(SqlException ex)
    {

        var transientErrorNumbers = new[] { -2, -1, 40197, 40501, 40613, 40540, 40544, 40549, 40550, 40551, 40552, 40553 };
        return ex.Errors.Cast<SqlError>().Any(e => transientErrorNumbers.Contains(e.Number));
    }

    private bool ValidateOpinion(OpinionRaw opinion)
    {

        if (string.IsNullOrWhiteSpace(opinion.Comentario) || opinion.Comentario.Length < 3)
            return false;


        if (opinion.Puntaje.HasValue && (opinion.Puntaje < 0 || opinion.Puntaje > 100))
            return false;


        if (opinion.Rating.HasValue && (opinion.Rating < 1 || opinion.Rating > 5))
            return false;


            if (opinion.FechaOrigen < new DateTime(2020, 1, 1))
                return false;

        return true;
    }
}
