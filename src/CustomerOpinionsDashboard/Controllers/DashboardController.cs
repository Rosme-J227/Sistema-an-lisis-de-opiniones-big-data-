using System;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using CustomerOpinionsDashboard.Models;
using CustomerOpinionsDashboard.Services;

namespace CustomerOpinionsDashboard.Controllers;

[Route("")]
[Route("api/[controller]")]
public class DashboardController : Controller
{
    private readonly IDashboardRepository _repo;
    private readonly ILogger<DashboardController>? _logger;

    public DashboardController(IDashboardRepository repo, ILogger<DashboardController>? logger = null)
    {
        _repo = repo;
        _logger = logger;
    }



    public async Task<IActionResult> Index(
        DateTime? from = null,
        DateTime? to = null,
        string? searchText = null,
        string? sentimentFilter = null,
        int pageNumber = 1,
        int pageSize = 10)
    {
        try
        {

            var total = await _repo.GetTotalCommentsAsync(from, to);
            var nps = await _repo.GetNPSAsync(from, to);
            var csat = await _repo.GetCSATAsync(from, to);
            var topProds = await _repo.GetTopProductsAsync(1);
            double avgSatisfaction = topProds.Any() ? (topProds.First().AvgSatisfaction ?? 0) : 0;


            var sentiments = await _repo.GetSentimentCountsAsync(from, to);
            var topProducts = await _repo.GetTopProductsAsync(8);
            var trend = await _repo.GetTrendByMonthAsync(12);
            var channels = await _repo.GetOpinionsByChannelAsync();
            var (opinions, opinionsTotal) = await _repo.GetDetailedOpinionAsync(
                pageNumber, pageSize, searchText, sentimentFilter, from, to);

            var viewModel = new DashboardViewModel
            {
                Kpi = new KpiViewModel
                {
                    TotalComments = total,
                    NPS = nps,
                    CSAT = csat,
                    AvgSatisfaction = avgSatisfaction,
                    PositiveSentiments = sentiments.FirstOrDefault(s => s.Classification == "Positive").Count,
                    NegativeSentiments = sentiments.FirstOrDefault(s => s.Classification == "Negative").Count,
                    NeutralSentiments = sentiments.FirstOrDefault(s => s.Classification == "Neutral").Count
                },
                Filter = new FilterViewModel 
                { 
                    FromDate = from, 
                    ToDate = to, 
                    SearchText = searchText, 
                    SentimentFilter = sentimentFilter, 
                    PageNumber = pageNumber, 
                    PageSize = pageSize 
                },
                SentimentData = sentiments.Select(s => new ChartDataViewModel 
                { 
                    Label = s.Classification, 
                    Value = s.Count 
                }).ToList(),
                SatisfactionData = topProducts.Select(p => new ChartDataViewModel 
                { 
                    Label = p.ProductName, 
                    Value = (int)(p.AvgSatisfaction ?? 0) 
                }).ToList(),
                TrendData = trend.Select(t => new TrendDto 
                { 
                    Period = t.Period, 
                    TotalComments = t.TotalComments, 
                    AvgSatisfaction = t.AvgSatisfaction ?? 0
                }).ToList(),
                ChannelData = channels.Select(c => new ChartDataViewModel 
                { 
                    Label = c.Channel, 
                    Value = c.Count 
                }).ToList(),
                Opinions = new DetailedOpinionsViewModel
                {
                    Opinions = opinions.Select(o => new DetailedOpinionRowViewModel
                    {
                        OpinionId = o.OpinionId,
                        ProductName = o.ProductName,
                        ClientName = o.ClientName,
                        Sentiment = o.Sentiment,
                        Comment = o.Comment,
                        Rating = o.Rating,
                        Satisfaction = o.Satisfaction,
                        Channel = o.Channel,
                        Date = o.Date,
                        Score = o.Score
                    }).ToList(),
                    Total = opinionsTotal,
                    PageNumber = pageNumber,
                    PageSize = pageSize
                }
            };

            return View(viewModel);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error loading dashboard");
            return StatusCode(500);
        }
    }



    [HttpGet("api/dashboard/total")]
    public async Task<IActionResult> TotalComments(DateTime? from = null, DateTime? to = null)
    {
        try
        {
            var total = await _repo.GetTotalCommentsAsync(from, to);
            return Ok(new { total });
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting total comments");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/sentiment")]
    public async Task<IActionResult> Sentiment(DateTime? from = null, DateTime? to = null)
    {
        try
        {
            var data = await _repo.GetSentimentCountsAsync(from, to);
            return Ok(data.Select(d => new { classification = d.Classification, count = d.Count }));
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting sentiment");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/top-products")]
    public async Task<IActionResult> TopProducts(int top = 10)
    {
        try
        {
            var data = await _repo.GetTopProductsAsync(top);
            return Ok(data.Select(d => new 
            { 
                productName = d.ProductName, 
                totalComments = d.TotalComments, 
                avgSatisfaction = d.AvgSatisfaction 
            }));
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting top products");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/trend")]
    public async Task<IActionResult> Trend(int months = 12)
    {
        try
        {
            var data = await _repo.GetTrendByMonthAsync(months);
            return Ok(data.Select(d => new 
            { 
                period = d.Period, 
                totalComments = d.TotalComments, 
                avgSatisfaction = d.AvgSatisfaction 
            }));
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting trend");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/channels")]
    public async Task<IActionResult> Channels()
    {
        try
        {
            var data = await _repo.GetOpinionsByChannelAsync();
            return Ok(data.Select(d => new 
            { 
                channel = d.Channel, 
                count = d.Count, 
                avgSatisfaction = d.AvgSatisfaction 
            }));
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting channels");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/opinions")]
    public async Task<IActionResult> DetailedOpinions(
        int pageNumber = 1,
        int pageSize = 50,
        string? searchText = null,
        string? sentimentFilter = null,
        DateTime? fromDate = null,
        DateTime? toDate = null)
    {
        try
        {
            var (items, total) = await _repo.GetDetailedOpinionAsync(
                pageNumber, pageSize, searchText, sentimentFilter, fromDate, toDate);

            var data = items.Select(i => new
            {
                opinionId = i.OpinionId,
                productName = i.ProductName,
                clientName = i.ClientName,
                sentiment = i.Sentiment,
                comment = i.Comment,
                rating = i.Rating,
                satisfaction = i.Satisfaction,
                channel = i.Channel,
                date = i.Date,
                score = i.Score
            }).ToList();

            return Ok(new
            {
                items = data,
                total,
                pageNumber,
                pageSize,
                totalPages = (total + pageSize - 1) / pageSize
            });
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting detailed opinions");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/nps")]
    public async Task<IActionResult> GetNPS(DateTime? from = null, DateTime? to = null)
    {
        try
        {
            var nps = await _repo.GetNPSAsync(from, to);
            return Ok(new { nps });
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error calculating NPS");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/csat")]
    public async Task<IActionResult> GetCSAT(DateTime? from = null, DateTime? to = null)
    {
        try
        {
            var csat = await _repo.GetCSATAsync(from, to);
            return Ok(new { csat });
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error calculating CSAT");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/kpis")]
    public async Task<IActionResult> GetKpis(DateTime? from = null, DateTime? to = null)
    {
        try
        {
            var total = await _repo.GetTotalCommentsAsync(from, to);
            var nps = await _repo.GetNPSAsync(from, to);
            var csat = await _repo.GetCSATAsync(from, to);
            var sentiments = await _repo.GetSentimentCountsAsync(from, to);
            var topProducts = await _repo.GetTopProductsAsync(1);
            double avgSatisfaction = topProducts.Any() ? (topProducts.First().AvgSatisfaction ?? 0) : 0;

            var kpi = new KpiViewModel
            {
                TotalComments = total,
                NPS = nps,
                CSAT = csat,
                AvgSatisfaction = avgSatisfaction,
                PositiveSentiments = sentiments.FirstOrDefault(s => s.Classification == "Positive").Count,
                NegativeSentiments = sentiments.FirstOrDefault(s => s.Classification == "Negative").Count,
                NeutralSentiments = sentiments.FirstOrDefault(s => s.Classification == "Neutral").Count
            };

            return Ok(kpi);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting KPIs");
            return StatusCode(500, new { error = ex.Message });
        }
    }



    [HttpGet("api/dashboard/all")]
    public async Task<IActionResult> GetAll(DateTime? from = null, DateTime? to = null)
    {
        try
        {
            var sentiments = await _repo.GetSentimentCountsAsync(from, to);
            var topProducts = await _repo.GetTopProductsAsync(8);
            var trend = await _repo.GetTrendByMonthAsync(12);
            var channels = await _repo.GetOpinionsByChannelAsync();

            var total = await _repo.GetTotalCommentsAsync(from, to);
            var nps = await _repo.GetNPSAsync(from, to);
            var csat = await _repo.GetCSATAsync(from, to);

            return Ok(new
            {
                sentiment = sentiments.Select(s => new { classification = s.Classification, count = s.Count }),
                topProducts = topProducts.Select(p => new 
                { 
                    productName = p.ProductName, 
                    totalComments = p.TotalComments, 
                    avgSatisfaction = p.AvgSatisfaction 
                }),
                trend = trend.Select(t => new 
                { 
                    period = t.Period, 
                    totalComments = t.TotalComments, 
                    avgSatisfaction = t.AvgSatisfaction 
                }),
                channels = channels.Select(c => new 
                { 
                    channel = c.Channel, 
                    count = c.Count, 
                    avgSatisfaction = c.AvgSatisfaction 
                }),
                kpis = new 
                { 
                    totalComments = total, 
                    nps, 
                    csat,
                    avgSatisfaction = topProducts.Any() ? topProducts.First().AvgSatisfaction : 0
                }
            });
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Error getting all dashboard data");
            return StatusCode(500, new { error = ex.Message });
        }
    }
}
