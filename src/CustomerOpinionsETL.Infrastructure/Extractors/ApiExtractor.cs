using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using CustomerOpinionsETL.Core.Entities;
using CustomerOpinionsETL.Core.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace CustomerOpinionsETL.Infrastructure.Extractors;

public class ApiExtractor : IExtractor
{
    private readonly HttpClient _http;
    private readonly int _maxRetries;
    private readonly int _timeoutSeconds;
    private readonly int _maxRequestsPerMinute;
    private readonly ILogger<ApiExtractor>? _logger;
    private readonly IConfiguration? _config;
    private DateTime _lastRequestTime = DateTime.MinValue;
    private int _requestsInCurrentMinute = 0;

    public ApiExtractor(
        HttpClient http,
        int maxRetries = 3,
        int timeoutSeconds = 30,
        int maxRequestsPerMinute = 60,
        ILogger<ApiExtractor>? logger = null,
        IConfiguration? config = null)
    {
        _http = http;
        _maxRetries = maxRetries;
        _timeoutSeconds = timeoutSeconds;
        _maxRequestsPerMinute = maxRequestsPerMinute;
        _logger = logger;
        _config = config;


        _http.Timeout = TimeSpan.FromSeconds(_config?.GetValue<int>("Api:Timeout") ?? _timeoutSeconds);
    }

    public async Task<List<OpinionRaw>> ExtractAsync(CancellationToken ct = default)
    {
        _logger?.LogInformation("Starting API extraction");

        var results = new List<OpinionRaw>();
        int page = 1;
        const int pageSize = 500;
        int totalExtracted = 0;
        int totalErrors = 0;

        try
        {
            while (!ct.IsCancellationRequested)
            {
                try
                {

                    await ApplyRateLimitingAsync(ct);

                    var resp = await GetPageWithRetryAsync(page, pageSize, ct);

                    if (resp?.Items == null || resp.Items.Count == 0)
                    {
                        _logger?.LogInformation("API extraction completed. Total extracted: {Total}, Total errors: {Errors}",
                            totalExtracted, totalErrors);
                        break;
                    }

                    foreach (var item in resp.Items)
                    {
                        try
                        {
                            if (ValidateApiItem(item))
                            {
                                var opinion = MapApiItemToOpinion(item);
                                totalExtracted++;
                                results.Add(opinion);
                            }
                            else
                            {
                                totalErrors++;
                                _logger?.LogDebug("API item validation failed for IdOrigen: {IdOrigen}", item.IdOrigen);
                            }
                        }
                        catch (Exception ex)
                        {
                            totalErrors++;
                            _logger?.LogWarning(ex, "Error processing API item: {Message}", ex.Message);
                        }
                    }

                    page++;
                }
                catch (HttpRequestException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound
                    || ex.StatusCode == System.Net.HttpStatusCode.BadRequest)
                {

                    _logger?.LogInformation("API pagination ended at page {Page} ({Status}). Total extracted: {Total}",
                        page, ex.StatusCode, totalExtracted);
                    break;
                }
                catch (Exception ex)
                {
                    totalErrors++;
                    _logger?.LogError(ex, "Error fetching page {Page} from API: {Message}", page, ex.Message);
                    throw;
                }
            }
        }
        finally
        {
            _logger?.LogInformation("API extraction finished. Total extracted: {Total}, Total errors: {Errors}",
                totalExtracted, totalErrors);
        }

        return results;
    }

    private async Task<ApiPage?> GetPageWithRetryAsync(int page, int pageSize, CancellationToken ct)
    {
        int attempt = 0;
        while (attempt < _maxRetries)
        {
            try
            {
                attempt++;
                _logger?.LogDebug("API request attempt {Attempt} of {MaxRetries} for page {Page}",
                    attempt, _maxRetries, page);

                string url = $"/api/reviews?page={page}&pageSize={pageSize}";
                var response = await _http.GetAsync(url, HttpCompletionOption.ResponseContentRead, ct);


                if (!response.IsSuccessStatusCode)
                {
                    if (response.StatusCode == HttpStatusCode.TooManyRequests)
                    {

                        int retryAfterSeconds = GetRetryAfterSeconds(response);
                        _logger?.LogWarning("API rate limited. Waiting {Seconds} seconds before retry", retryAfterSeconds);
                        await Task.Delay(TimeSpan.FromSeconds(retryAfterSeconds), ct);
                        continue;
                    }

                    if (response.StatusCode == HttpStatusCode.BadRequest || response.StatusCode == HttpStatusCode.NotFound)
                    {
                        _logger?.LogWarning("API returned {StatusCode} for page {Page}", response.StatusCode, page);
                        throw new HttpRequestException($"API returned {response.StatusCode}", null, response.StatusCode);
                    }

                    if (response.StatusCode == HttpStatusCode.Unauthorized || response.StatusCode == HttpStatusCode.Forbidden)
                    {
                        _logger?.LogError("API authentication failed: {StatusCode}", response.StatusCode);
                        throw new HttpRequestException($"API authentication failed: {response.StatusCode}", null, response.StatusCode);
                    }

                    if (response.StatusCode == HttpStatusCode.InternalServerError || response.StatusCode == HttpStatusCode.ServiceUnavailable)
                    {

                        if (attempt < _maxRetries)
                        {
                            int delayMs = (int)Math.Pow(2, attempt) * 1000;
                            _logger?.LogWarning("API returned {StatusCode}. Retrying in {DelayMs}ms", response.StatusCode, delayMs);
                            await Task.Delay(delayMs, ct);
                            continue;
                        }
                        else
                        {
                            throw new HttpRequestException($"API server error after {_maxRetries} attempts: {response.StatusCode}");
                        }
                    }

                    throw new HttpRequestException($"API request failed with status {response.StatusCode}");
                }


                var content = await response.Content.ReadAsStringAsync(ct);
                if (string.IsNullOrWhiteSpace(content))
                {
                    _logger?.LogWarning("API returned empty content for page {Page}", page);
                    throw new InvalidOperationException("API returned empty content");
                }

                try
                {
                    var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
                    var result = JsonSerializer.Deserialize<ApiPage>(content, options);

                    if (result == null)
                    {
                        _logger?.LogWarning("JSON deserialization resulted in null for page {Page}", page);
                        throw new InvalidOperationException("JSON deserialization failed");
                    }

                    _logger?.LogDebug("Successfully fetched page {Page} with {ItemCount} items", page, result.Items?.Count ?? 0);
                    return result;
                }
                catch (JsonException ex)
                {
                    _logger?.LogError(ex, "Invalid JSON response from API for page {Page}: {Message}", page, ex.Message);
                    throw new InvalidOperationException($"Invalid JSON response from API", ex);
                }
            }
            catch (TaskCanceledException ex)
            {
                _logger?.LogError(ex, "API request timeout for page {Page} on attempt {Attempt}", page, attempt);
                if (attempt < _maxRetries)
                {
                    await Task.Delay(1000 * attempt, ct);
                }
                else
                {
                    throw;
                }
            }
            catch (HttpRequestException ex)
            {
                _logger?.LogError(ex, "HTTP error on page {Page} attempt {Attempt}: {Message}", page, attempt, ex.Message);
                throw;
            }
        }

        throw new InvalidOperationException($"API request failed after {_maxRetries} attempts");
    }

    private async Task ApplyRateLimitingAsync(CancellationToken ct)
    {
        var now = DateTime.UtcNow;


        if ((now - _lastRequestTime).TotalMinutes >= 1)
        {
            _requestsInCurrentMinute = 0;
            _lastRequestTime = now;
        }


        if (_requestsInCurrentMinute >= _maxRequestsPerMinute)
        {
            int delayMs = (int)((DateTime.UtcNow.AddMinutes(1) - now).TotalMilliseconds);
            _logger?.LogInformation("Rate limit reached. Waiting {DelayMs}ms before next request", delayMs);
            await Task.Delay(delayMs, ct);
            _requestsInCurrentMinute = 0;
        }

        _requestsInCurrentMinute++;
    }

    private int GetRetryAfterSeconds(HttpResponseMessage response)
    {
        if (response.Headers.RetryAfter?.Delta.HasValue == true)
            return (int)response.Headers.RetryAfter.Delta.Value.TotalSeconds;

        return 60;
    }

    private bool ValidateApiItem(ApiItem item)
    {

        if (string.IsNullOrWhiteSpace(item.Comentario) || item.Comentario.Length < 3)
            return false;


        if (item.Puntaje.HasValue && (item.Puntaje < 0 || item.Puntaje > 100))
            return false;


        if (item.Rating.HasValue && (item.Rating < 1 || item.Rating > 5))
            return false;


        if (item.Fecha < new DateTime(2020, 1, 1))
            return false;


        if (string.IsNullOrWhiteSpace(item.IdOrigen) && !item.ClienteId.HasValue)
            return false;

        return true;
    }

    private OpinionRaw MapApiItemToOpinion(ApiItem item)
    {
        return new OpinionRaw
        {
            Fuente = item.Fuente ?? "API_SOCIAL_MEDIA",
            IdOrigen = item.IdOrigen,
            ClienteId = item.ClienteId,
            CodigoProducto = item.ProductoId?.ToString(),
            Comentario = CleanComment(item.Comentario),
            Puntaje = item.Puntaje,
            Rating = item.Rating,
            FechaOrigen = item.Fecha
        };
    }

    private string CleanComment(string? comment)
    {
        if (string.IsNullOrWhiteSpace(comment))
            return "";


        string cleaned = Regex.Replace(comment, @"[\x00-\x1F\x7F-\x9F]", " ");
        cleaned = Regex.Replace(cleaned, @"\s+", " ");
        return cleaned.Trim();
    }

    private class ApiPage
    {
        public int Page { get; set; }
        public int PageSize { get; set; }
        [SuppressMessage("Usage", "CA2227:Collection properties should be read only", Justification = "JSON deserialization")]
        public List<ApiItem>? Items { get; set; }
    }

    private class ApiItem
    {
        public string? IdOrigen { get; set; }
        public int? ClienteId { get; set; }
        public int? ProductoId { get; set; }
        public string? Fuente { get; set; }
        public DateTime Fecha { get; set; }
        public string? Comentario { get; set; }
        public int? Puntaje { get; set; }
        public int? Rating { get; set; }
    }
}
