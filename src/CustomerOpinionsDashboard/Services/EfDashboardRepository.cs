using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using CustomerOpinionsDashboard.Data;
using CustomerOpinionsDashboard.Models;
using Microsoft.Extensions.Logging;

namespace CustomerOpinionsDashboard.Services;

public class EfDashboardRepository : IDashboardRepository
{
    private readonly DwContext _db;
    private readonly ILogger<EfDashboardRepository>? _logger;

    public EfDashboardRepository(DwContext db, ILogger<EfDashboardRepository>? logger = null)
    {
        _db = db;
        _logger = logger;
    }

    public async Task<int> GetTotalCommentsAsync(DateTime? from = null, DateTime? to = null)
    {
        var q = _db.FactOpiniones.AsQueryable();
        if (from.HasValue)
            q = q.Where(f => f.Fecha_carga >= from.Value);
        if (to.HasValue)
            q = q.Where(f => f.Fecha_carga <= to.Value);
        return await q.CountAsync();
    }

    public async Task<IEnumerable<(string Classification, int Count)>> GetSentimentCountsAsync(DateTime? from = null, DateTime? to = null)
    {
        var q = _db.FactOpiniones
            .Include(f => f.Sentimiento)
            .Include(f => f.Tiempo)
            .AsQueryable();
        if (from.HasValue)
        {
            var fromDate = DateOnly.FromDateTime(from.Value);
            q = q.Where(f => f.Tiempo.Fecha >= fromDate);
        }
        if (to.HasValue)
        {
            var toDate = DateOnly.FromDateTime(to.Value);
            q = q.Where(f => f.Tiempo.Fecha <= toDate);
        }
        var grouped = await q
            .GroupBy(f => f.Sentimiento.Clasificacion)
            .Select(g => new { Classification = g.Key, Count = g.Count() })
            .ToListAsync();
        return grouped.Select(x => (x.Classification ?? "Unknown", x.Count));
    }

    public async Task<IEnumerable<(string ProductName, int TotalComments, double? AvgSatisfaction)>> GetTopProductsAsync(int top = 10)
    {
        var q = _db.FactOpiniones
            .Include(f => f.Producto)
            .AsQueryable();
        var grouped = await q
            .GroupBy(f => f.Producto.Nombre_producto)
            .Select(g => new
            {
                ProductName = g.Key,
                TotalComments = g.Count(),
                AvgSatisfaction = g.Average(x => (double?)x.Puntaje_Satisfaccion)
            })
            .OrderByDescending(x => x.TotalComments)
            .Take(top)
            .ToListAsync();
        return grouped.Select(x => (x.ProductName ?? "Unknown", x.TotalComments, x.AvgSatisfaction));
    }

    public async Task<IEnumerable<(string Period, int TotalComments, double? AvgSatisfaction)>> GetTrendByMonthAsync(int months = 12)
    {
        var q = _db.FactOpiniones
            .Include(f => f.Tiempo)
            .AsQueryable();
        var grouped = await q
            .GroupBy(f => f.Tiempo.Mes_año)
            .Select(g => new
            {
                Period = g.Key,
                TotalComments = g.Count(),
                AvgSatisfaction = g.Average(x => (double?)x.Puntaje_Satisfaccion)
            })
            .OrderByDescending(x => x.Period)
            .Take(months)
            .ToListAsync();
        return grouped.Select(x => (x.Period ?? "Unknown", x.TotalComments, x.AvgSatisfaction)).Reverse();
    }

    public async Task<IEnumerable<(string Channel, int Count, double? AvgSatisfaction)>> GetOpinionsByChannelAsync()
    {
        try
        {
            var query = from f in _db.FactOpiniones
                        join src in _db.DimFuentes on f.Fuente_id equals src.Fuente_id
                        group f by src.TipoFuente into g
                        select new
                        {
                            Channel = g.Key,
                            Count = g.Count(),
                            AvgSatisfaction = g.Average(x => (double?)x.Puntaje_Satisfaccion)
                        };

            var results = await query
                .OrderByDescending(x => x.Count)
                .ToListAsync();

            return results.Select(x => (x.Channel ?? "Unknown", x.Count, x.AvgSatisfaction)).ToList();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in GetOpinionsByChannelAsync");
            throw;
        }
    }

    public async Task<IEnumerable<(string ProductName, int Total, int Positive, int Negative, int Neutral, double? AvgScore)>> GetSatisfactionByProductAsync()
    {
        try
        {
            var query = from f in _db.FactOpiniones
                        join p in _db.DimProductos on f.Producto_id equals p.Producto_id
                        join s in _db.DimSentimientos on f.Sentimiento_id equals s.Sentimiento_id
                        group new { f, s } by p.Nombre_producto into g
                        select new
                        {
                            ProductName = g.Key,
                            Total = g.Count(),
                            Positive = g.Count(x => x.s.Clasificacion == "Positive"),
                            Negative = g.Count(x => x.s.Clasificacion == "Negative"),
                            Neutral = g.Count(x => x.s.Clasificacion == "Neutral"),
                            AvgScore = g.Average(x => (double?)x.f.Puntaje_Satisfaccion)
                        };

            var results = await query
                .OrderByDescending(x => x.Total)
                .ToListAsync();

            return results.Select(x => (
                x.ProductName ?? "Unknown",
                x.Total,
                x.Positive,
                x.Negative,
                x.Neutral,
                x.AvgScore
            )).ToList();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in GetSatisfactionByProductAsync");
            throw;
        }
    }

    public async Task<(List<DetailedOpinionDto> Items, int Total)> GetDetailedOpinionAsync(
        int pageNumber = 1, 
        int pageSize = 50,
        string? searchText = null,
        string? sentimentFilter = null,
        DateTime? fromDate = null,
        DateTime? toDate = null)
    {
        try
        {
            var query = from f in _db.FactOpiniones
                        join p in _db.DimProductos on f.Producto_id equals p.Producto_id
                        join c in _db.DimClientes on f.Cliente_id equals c.Cliente_id
                        join s in _db.DimSentimientos on f.Sentimiento_id equals s.Sentimiento_id
                        join src in _db.DimFuentes on f.Fuente_id equals src.Fuente_id
                        join t in _db.DimTiempos on f.Tiempo_id equals t.Tiempo_id
                        select new { f, p, c, s, src, t };

            if (!string.IsNullOrWhiteSpace(searchText))
                query = query.Where(x => x.f.Comentario != null && x.f.Comentario.Contains(searchText));

            if (!string.IsNullOrWhiteSpace(sentimentFilter))
                query = query.Where(x => x.s.Clasificacion == sentimentFilter);

            if (fromDate.HasValue)
            {
                var fd = DateOnly.FromDateTime(fromDate.Value);
                query = query.Where(x => x.t.Fecha >= fd);
            }

            if (toDate.HasValue)
            {
                var td = DateOnly.FromDateTime(toDate.Value);
                query = query.Where(x => x.t.Fecha <= td);
            }

            var total = await query.CountAsync();

            var results = await query
                .OrderByDescending(x => x.f.Fecha_carga)
                .Skip((pageNumber - 1) * pageSize)
                .Take(pageSize)
                .Select(x => new DetailedOpinionDto
                {
                    OpinionId = x.f.Opinion_id,
                    ProductName = x.p.Nombre_producto,
                    ClientName = x.c.Nombre_Cliente,
                    Sentiment = x.s.Clasificacion,
                    Comment = x.f.Comentario,
                    Rating = x.f.Rating,
                    Satisfaction = x.f.Puntaje_Satisfaccion,
                    Channel = x.src.TipoFuente,
                    Date = x.t.Fecha.ToDateTime(TimeOnly.MinValue),
                    Score = x.f.Score_sentimiento.HasValue ? (double?)x.f.Score_sentimiento : null
                })
                .ToListAsync();

            return (results, total);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in GetDetailedOpinionAsync");
            throw;
        }
    }

    public async Task<double> GetNPSAsync(DateTime? from = null, DateTime? to = null)
    {
        try
        {
            var query = _db.FactOpiniones.AsQueryable();

            if (from.HasValue)
                query = query.Where(f => f.Fecha_carga >= from.Value);

            if (to.HasValue)
                query = query.Where(f => f.Fecha_carga <= to.Value);

            var total = await query.CountAsync();
            if (total == 0) return 0;

            var promoters = await query.Where(f => f.Rating >= 4).CountAsync();
            var detractors = await query.Where(f => f.Rating <= 2).CountAsync();

            var nps = ((promoters - detractors) / (double)total) * 100;
            
            _logger?.LogDebug("GetNPS: Total={Total}, Promoters={Promoters}, Detractors={Detractors}, NPS={NPS:F2}",
                total, promoters, detractors, nps);

            return Math.Round(nps, 2);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in GetNPSAsync");
            throw;
        }
    }

    public async Task<double> GetCSATAsync(DateTime? from = null, DateTime? to = null)
    {
        try
        {
            var query = _db.FactOpiniones.AsQueryable();

            if (from.HasValue)
                query = query.Where(f => f.Fecha_carga >= from.Value);

            if (to.HasValue)
                query = query.Where(f => f.Fecha_carga <= to.Value);

            var total = await query.CountAsync();
            if (total == 0) return 0;

            var satisfied = await query
                .Where(f => (f.Rating ?? 0) >= 4 || (f.Puntaje_Satisfaccion ?? 0) >= 70)
                .CountAsync();

            var csat = (satisfied / (double)total) * 100;
            
            _logger?.LogDebug("GetCSAT: Total={Total}, Satisfied={Satisfied}, CSAT={CSAT:F2}",
                total, satisfied, csat);

            return Math.Round(csat, 2);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in GetCSATAsync");
            throw;
        }
    }

    public async Task<IEnumerable<(string ProductName, string Period, int Count, double? AvgSatisfaction)>> GetTrendByProductAsync()
    {
        try
        {
            var query = from f in _db.FactOpiniones
                        join p in _db.DimProductos on f.Producto_id equals p.Producto_id
                        join t in _db.DimTiempos on f.Tiempo_id equals t.Tiempo_id
                        group f by new { p.Nombre_producto, t.Mes_año } into g
                        select new
                        {
                            ProductName = g.Key.Nombre_producto,
                            Period = g.Key.Mes_año,
                            Count = g.Count(),
                            AvgSatisfaction = g.Average(x => (double?)x.Puntaje_Satisfaccion)
                        };

            var results = await query
                .OrderBy(x => x.Period)
                .ThenBy(x => x.ProductName)
                .ToListAsync();

            return results.Select(x => (
                x.ProductName ?? "Unknown",
                x.Period ?? "Unknown",
                x.Count,
                x.AvgSatisfaction
            )).ToList();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in GetTrendByProductAsync");
            throw;
        }
    }

    public async Task<IEnumerable<(string ClientName, int OpinionCount, double? AvgSatisfaction)>> GetOpinionsByClientAsync(int top = 10)
    {
        try
        {
            var query = from f in _db.FactOpiniones
                        join c in _db.DimClientes on f.Cliente_id equals c.Cliente_id
                        group f by c.Nombre_Cliente into g
                        select new
                        {
                            ClientName = g.Key,
                            OpinionCount = g.Count(),
                            AvgSatisfaction = g.Average(x => (double?)x.Puntaje_Satisfaccion)
                        };

            var results = await query
                .OrderByDescending(x => x.OpinionCount)
                .Take(top)
                .ToListAsync();

            return results.Select(x => (
                x.ClientName ?? "Unknown",
                x.OpinionCount,
                x.AvgSatisfaction
            )).ToList();
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error in GetOpinionsByClientAsync");
            throw;
        }
    }
}
