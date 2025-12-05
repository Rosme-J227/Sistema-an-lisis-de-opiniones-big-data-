using CustomerOpinionsETL.Core.Entities;
namespace CustomerOpinionsETL.Core.Interfaces;
public interface ISentimentAnalyzer
{



    (string Classification, double Score) Analyze(string? text);
}
