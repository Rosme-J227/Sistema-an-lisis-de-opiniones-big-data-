using System;
using System.Collections.Generic;
namespace CustomerOpinionsDashboard.Models;




public class KpiViewModel
{
    public int TotalComments { get; set; }
    public double NPS { get; set; }
    public double CSAT { get; set; }
    public double AvgSatisfaction { get; set; }
    public int PositiveSentiments { get; set; }
    public int NegativeSentiments { get; set; }
    public int NeutralSentiments { get; set; }
}



public class ChartDataViewModel
{
    public string? Label { get; set; }
    public int Value { get; set; }
    public double? Percentage { get; set; }
    public string? Color { get; set; }
}



public class FilterViewModel
{
    public DateTime? FromDate { get; set; }
    public DateTime? ToDate { get; set; }
    public string? SearchText { get; set; }
    public string? SentimentFilter { get; set; }
    public string? ProductFilter { get; set; }
    public string? ChannelFilter { get; set; }
    public int PageNumber { get; set; } = 1;
    public int PageSize { get; set; } = 50;
}



public class DetailedOpinionsViewModel
{
    public List<DetailedOpinionRowViewModel> Opinions { get; set; } = new();
    public int Total { get; set; }
    public int PageNumber { get; set; }
    public int PageSize { get; set; }
    public int TotalPages => (Total + PageSize - 1) / PageSize;
}

public class DetailedOpinionRowViewModel
{
    public long OpinionId { get; set; }
    public string? ProductName { get; set; }
    public string? ClientName { get; set; }
    public string? Sentiment { get; set; }
    public string? Comment { get; set; }
    public int? Rating { get; set; }
    public int? Satisfaction { get; set; }
    public string? Channel { get; set; }
    public DateTime Date { get; set; }
    public double? Score { get; set; }
}



public class OpinionDto
{
    public long OpinionId { get; set; }
    public string? ProductName { get; set; }
    public string? ClientName { get; set; }
    public string? Sentiment { get; set; }
    public string? Comment { get; set; }
    public int? Rating { get; set; }
    public int? Satisfaction { get; set; }
    public string? Channel { get; set; }
    public DateTime Date { get; set; }
    public double? Score { get; set; }
}



public class TrendDto
{
    public string? Period { get; set; }
    public int TotalComments { get; set; }
    public double AvgSatisfaction { get; set; }
    public double? AvgNPS { get; set; }
    public double? AvgCSAT { get; set; }
    public int? PositiveCount { get; set; }
    public int? NegativeCount { get; set; }
    public int? NeutralCount { get; set; }
}




public class DashboardViewModel
{
    public KpiViewModel Kpi { get; set; } = new();
    public FilterViewModel Filter { get; set; } = new();
    public DetailedOpinionsViewModel Opinions { get; set; } = new();
    public List<ChartDataViewModel> SentimentData { get; set; } = new();
    public List<ChartDataViewModel> SatisfactionData { get; set; } = new();
    public List<TrendDto> TrendData { get; set; } = new();
    public List<ChartDataViewModel> ChannelData { get; set; } = new();
}
