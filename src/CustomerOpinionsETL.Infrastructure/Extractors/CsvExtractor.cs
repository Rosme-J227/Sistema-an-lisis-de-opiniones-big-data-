using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
using CustomerOpinionsETL.Core.Entities;
using CustomerOpinionsETL.Core.Interfaces;
using Microsoft.Extensions.Logging;

namespace CustomerOpinionsETL.Infrastructure.Extractors;

public class CsvExtractor : IExtractor
{
    private readonly string _filePath;
    private readonly ILogger<CsvExtractor>? _logger;

    public CsvExtractor(string filePath, ILogger<CsvExtractor>? logger = null)
    {
        _filePath = filePath;
        _logger = logger;
    }

    public async Task<List<OpinionRaw>> ExtractAsync(System.Threading.CancellationToken ct = default)
    {
        var results = new List<OpinionRaw>();

        if (!System.IO.File.Exists(_filePath))
        {
            _logger?.LogWarning("CSV file not found: {FilePath}", _filePath);
            return results;
        }

        _logger?.LogInformation("Starting CSV extraction from: {FilePath}", _filePath);

        var seenComments = new HashSet<string>();
        int rowsProcessed = 0;
        int rowsSkipped = 0;
        int rowsDuplicate = 0;

        try
        {
            using var reader = new System.IO.StreamReader(_filePath, Encoding.UTF8);
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);


            await csv.ReadAsync();
            csv.ReadHeader();
            var requiredColumns = new[] { "Id", "IdOrigen", "Nombre", "Email", "CodigoProducto", "Producto", "Comentario", "Fecha", "Puntaje", "Rating" };
            var headerRecord = csv.HeaderRecord ?? Array.Empty<string>();
            var missingColumns = requiredColumns.Where(col => !headerRecord.Contains(col)).ToList();

            if (missingColumns.Any() && missingColumns.Count >= 6)
            {
                _logger?.LogWarning("CSV header missing many expected columns. Found: {Headers}", string.Join(", ", headerRecord));
            }


            int rowNumber = 1;
            await foreach (var rec in csv.GetRecordsAsync<dynamic>().WithCancellation(ct))
            {
                rowNumber++;
                try
                {
                    if (!ValidateRow(rec))
                    {
                        _logger?.LogDebug("Row {RowNumber} failed validation", rowNumber);
                        rowsSkipped++;
                        continue;
                    }

                    var opinion = ExtractOpinion(rec, rowNumber);
                    if (opinion == null)
                    {
                        rowsSkipped++;
                        continue;
                    }


                    var commentHash = ComputeMD5Hash(opinion.Comentario ?? "");
                    if (!seenComments.Add(commentHash))
                    {
                        _logger?.LogDebug("Duplicate comment detected at row {RowNumber}", rowNumber);
                        rowsDuplicate++;
                        continue;
                    }

                    rowsProcessed++;
                    results.Add(opinion);
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Error processing row {RowNumber}", rowNumber);
                    rowsSkipped++;
                }
            }

            _logger?.LogInformation("CSV extraction completed. Processed: {Processed}, Skipped: {Skipped}, Duplicates: {Duplicates}",
                rowsProcessed, rowsSkipped, rowsDuplicate);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "CSV extraction failed: {Message}", ex.Message);
            throw;
        }

        return results;
    }

    private bool ValidateRow(dynamic row)
    {
        try
        {

            var email = (row.Email ?? "").ToString().Trim();
            var nombre = (row.Nombre ?? "").ToString().Trim();

            if (string.IsNullOrWhiteSpace(email) && string.IsNullOrWhiteSpace(nombre))
                return false;


            var comentario = (row.Comentario ?? "").ToString().Trim();
            if (string.IsNullOrWhiteSpace(comentario) || comentario.Length < 3)
                return false;


            if (row.Puntaje != null)
            {
                if (int.TryParse(row.Puntaje.ToString(), out int puntaje))
                {
                    if (puntaje < 0 || puntaje > 100)
                        return false;
                }
            }


            if (row.Rating != null)
            {
                if (int.TryParse(row.Rating.ToString(), out int rating))
                {
                    if (rating < 1 || rating > 5)
                        return false;
                }
            }

            return true;
        }
        catch
        {
            return false;
        }
    }

    private OpinionRaw? ExtractOpinion(dynamic row, int rowNumber)
    {
        try
        {
            string? id = row.Id ?? row.IdOrigen;
            if (string.IsNullOrWhiteSpace(id?.ToString()))
                id = $"CSV_{rowNumber}_{DateTime.UtcNow.Ticks}";

            int? puntaje = null;
            var puntajeStr = (row.Puntaje ?? "").ToString().Trim();
            if (int.TryParse(puntajeStr, out int pVal) && pVal >= 0 && pVal <= 100)
                puntaje = pVal;

            int? rating = null;
            var ratingStr = (row.Rating ?? "").ToString().Trim();
            if (int.TryParse(ratingStr, out int rVal) && rVal >= 1 && rVal <= 5)
                rating = rVal;

            DateTime fechaOrigen;
            var fechaStr = (row.Fecha ?? "").ToString().Trim();
            if (!DateTime.TryParse(fechaStr, out fechaOrigen))
                fechaOrigen = DateTime.UtcNow;


            var minDate = new DateTime(2020, 1, 1);
            if (fechaOrigen < minDate)
                fechaOrigen = minDate;

            var opinion = new OpinionRaw
            {
                Fuente = "CSV",
                IdOrigen = id?.ToString(),
                NombreCliente = CleanText(row.Nombre?.ToString()),
                Email = CleanText(row.Email?.ToString()),
                CodigoProducto = CleanText(row.CodigoProducto?.ToString() ?? row.Producto?.ToString()),
                Comentario = CleanComment(row.Comentario?.ToString()),
                Puntaje = puntaje,
                Rating = rating,
                FechaOrigen = fechaOrigen
            };

            return opinion;
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Failed to extract opinion from row {RowNumber}", rowNumber);
            return null;
        }
    }

    private string? CleanText(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return null;


        text = text.Trim();


        text = Regex.Replace(text, @"[\x00-\x08\x0B\x0C\x0E-\x1F]", "");


        text = Regex.Replace(text, @"\s+", " ");

        return string.IsNullOrWhiteSpace(text) ? null : text;
    }

    private string? CleanComment(string? comment)
    {
        if (string.IsNullOrWhiteSpace(comment))
            return null;


        comment = comment.Trim();


        comment = Regex.Replace(comment, @"[\x00-\x08\x0B\x0C\x0E-\x1F]", "");


        comment = Regex.Replace(comment, @"<[^>]*>", "");


        comment = Regex.Replace(comment, @"\s+", " ");


        if (comment.Length < 3)
            return null;

        return comment;
    }

    private string ComputeMD5Hash(string text)
    {
        using (var md5 = MD5.Create())
        {
            var hashBytes = md5.ComputeHash(Encoding.UTF8.GetBytes(text));
            return Convert.ToHexString(hashBytes);
        }
    }
}
