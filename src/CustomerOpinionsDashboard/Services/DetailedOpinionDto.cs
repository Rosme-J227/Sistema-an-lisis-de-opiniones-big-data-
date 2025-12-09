using System;

namespace CustomerOpinionsDashboard.Services;



public class DetailedOpinionDto
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
