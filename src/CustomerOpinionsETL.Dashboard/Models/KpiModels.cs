namespace CustomerOpinionsETL.Dashboard.Models;

public record SentimentCount
{
    public string? Classification { get; init; }
    public int Count { get; init; }
}

public record ProductSatisfaction
{
    public string? ProductName { get; init; }
    public int TotalComments { get; init; }
    public double? AvgSatisfaction { get; init; }
}

public record TrendPoint
{
    public string? Period { get; init; }
    public int TotalComments { get; init; }
    public double? AvgSatisfaction { get; init; }
}
