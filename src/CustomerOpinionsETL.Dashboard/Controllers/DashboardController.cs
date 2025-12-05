using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using CustomerOpinionsETL.Dashboard.Services;

namespace CustomerOpinionsETL.Dashboard.Controllers;

public class DashboardController : Controller
{
    private readonly IDwRepository _repo;
    public DashboardController(IDwRepository repo) => _repo = repo;

    public IActionResult Index() => View();

    [HttpGet("api/dashboard/total")]
    public async Task<IActionResult> TotalComments(DateTime? from = null, DateTime? to = null)
    {
        var total = await _repo.GetTotalCommentsAsync(from, to);
        return Ok(new { total });
    }

    [HttpGet("api/dashboard/sentiment")]
    public async Task<IActionResult> Sentiment(DateTime? from = null, DateTime? to = null)
    {
        var data = await _repo.GetSentimentCountsAsync(from, to);
        return Ok(data);
    }

    [HttpGet("api/dashboard/top-products")]
    public async Task<IActionResult> TopProducts(int top = 10)
    {
        var data = await _repo.GetTopProductsAsync(top);
        return Ok(data);
    }

    [HttpGet("api/dashboard/trend")]
    public async Task<IActionResult> Trend(int months = 12)
    {
        var data = await _repo.GetTrendByMonthAsync(months);
        return Ok(data);
    }
}
