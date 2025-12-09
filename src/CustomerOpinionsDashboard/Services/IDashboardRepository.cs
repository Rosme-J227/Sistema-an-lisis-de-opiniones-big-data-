using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using CustomerOpinionsDashboard.Models;
using CustomerOpinionsDashboard.Services;

namespace CustomerOpinionsDashboard.Services;

public interface IDashboardRepository
{
    Task<int> GetTotalCommentsAsync(System.DateTime? from = null, System.DateTime? to = null);
    Task<IEnumerable<(string Classification, int Count)>> GetSentimentCountsAsync(System.DateTime? from = null, System.DateTime? to = null);
    Task<IEnumerable<(string ProductName, int TotalComments, double? AvgSatisfaction)>> GetTopProductsAsync(int top = 10);
    Task<IEnumerable<(string Period, int TotalComments, double? AvgSatisfaction)>> GetTrendByMonthAsync(int months = 12);
    Task<IEnumerable<(string Channel, int Count, double? AvgSatisfaction)>> GetOpinionsByChannelAsync();
    Task<IEnumerable<(string ProductName, int Total, int Positive, int Negative, int Neutral, double? AvgScore)>> GetSatisfactionByProductAsync();
    Task<(List<DetailedOpinionDto> Items, int Total)> GetDetailedOpinionAsync(int pageNumber = 1, int pageSize = 50, string? searchText = null, string? sentimentFilter = null, System.DateTime? fromDate = null, System.DateTime? toDate = null);
    Task<double> GetNPSAsync(System.DateTime? from = null, System.DateTime? to = null);
    Task<double> GetCSATAsync(System.DateTime? from = null, System.DateTime? to = null);
    Task<IEnumerable<(string ProductName, string Period, int Count, double? AvgSatisfaction)>> GetTrendByProductAsync();
    Task<IEnumerable<(string ClientName, int OpinionCount, double? AvgSatisfaction)>> GetOpinionsByClientAsync(int top = 10);
}

