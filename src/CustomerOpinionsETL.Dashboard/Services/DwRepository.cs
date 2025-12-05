using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using CustomerOpinionsETL.Dashboard.Models;

namespace CustomerOpinionsETL.Dashboard.Services;

public interface IDwRepository
{
    Task<int> GetTotalCommentsAsync(DateTime? from = null, DateTime? to = null);
    Task<IEnumerable<SentimentCount>> GetSentimentCountsAsync(DateTime? from = null, DateTime? to = null);
    Task<IEnumerable<ProductSatisfaction>> GetTopProductsAsync(int top = 10);
    Task<IEnumerable<TrendPoint>> GetTrendByMonthAsync(int months = 12);
}

public class DwRepository : IDwRepository
{
    private readonly string _conn;
    public DwRepository(string connectionString) => _conn = connectionString;

    public async Task<int> GetTotalCommentsAsync(DateTime? from = null, DateTime? to = null)
    {
        using var con = new SqlConnection(_conn);
        await con.OpenAsync();
        var sql = @"SELECT COUNT(1) FROM Fact.FactOpiniones f
JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
WHERE (@From IS NULL OR t.Fecha >= @From) AND (@To IS NULL OR t.Fecha <= @To)";
        return await con.ExecuteScalarAsync<int>(sql, new { From = from, To = to });
    }

    public async Task<IEnumerable<SentimentCount>> GetSentimentCountsAsync(DateTime? from = null, DateTime? to = null)
    {
        using var con = new SqlConnection(_conn);
        await con.OpenAsync();
        var sql = @"SELECT s.Clasificacion AS Classification, COUNT(1) AS Count FROM Fact.FactOpiniones f
JOIN Dimension.DimSentimiento s ON f.Sentimiento_id = s.Sentimiento_id
JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
WHERE (@From IS NULL OR t.Fecha >= @From) AND (@To IS NULL OR t.Fecha <= @To)
GROUP BY s.Clasificacion";
        return await con.QueryAsync<SentimentCount>(sql, new { From = from, To = to });
    }

    public async Task<IEnumerable<ProductSatisfaction>> GetTopProductsAsync(int top = 10)
    {
        using var con = new SqlConnection(_conn);
        await con.OpenAsync();
        var sql = @"SELECT p.Nombre_producto AS ProductName, COUNT(1) AS TotalComments, AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS AvgSatisfaction
FROM Fact.FactOpiniones f
JOIN Dimension.DimProducto p ON f.Producto_id = p.Producto_id
GROUP BY p.Nombre_producto
ORDER BY TotalComments DESC
OFFSET 0 ROWS FETCH NEXT @Top ROWS ONLY";
        return await con.QueryAsync<ProductSatisfaction>(sql, new { Top = top });
    }

    public async Task<IEnumerable<TrendPoint>> GetTrendByMonthAsync(int months = 12)
    {
        using var con = new SqlConnection(_conn);
        await con.OpenAsync();
        var sql = @"SELECT t.Mes_año AS Period, COUNT(1) AS TotalComments, AVG(CAST(f.Puntaje_Satisfaccion AS FLOAT)) AS AvgSatisfaction
FROM Fact.FactOpiniones f
JOIN Dimension.DimTiempo t ON f.Tiempo_id = t.Tiempo_id
GROUP BY t.Mes_año, DATEFROMPARTS(t.Año, t.Mes, 1)
ORDER BY DATEFROMPARTS(t.Año, t.Mes, 1) DESC";
        var rows = (await con.QueryAsync<TrendPoint>(sql)).ToList();
        return rows.Take(months).Reverse();
    }
}
