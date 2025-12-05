using System;
using System.Linq;
using CustomerOpinionsETL.Core.Interfaces;
namespace CustomerOpinionsETL.Infrastructure.Sentiment;
public class SimpleSentimentAnalyzer : ISentimentAnalyzer
{

    private static readonly string[] PositiveWords = new[] { "bueno", "excelente", "genial", "perfecto", "agradable", "recomend", "love", "great", "fantast" };
    private static readonly string[] NegativeWords = new[] { "malo", "terrible", "horrible", "defecto", "devolver", "hate", "bad", "peor", "problema" };

    public (string Classification, double Score) Analyze(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return ("Neutral", 0.0);

        var lower = text.ToLowerInvariant();
        int pos = PositiveWords.Count(w => lower.Contains(w));
        int neg = NegativeWords.Count(w => lower.Contains(w));

        if (pos == 0 && neg == 0)
            return ("Neutral", 0.0);

        var score = (pos - neg) / (double)(pos + neg);
        var cls = score > 0 ? "Positive" : score < 0 ? "Negative" : "Neutral";
        return (cls, Math.Round(score, 2));
    }
}
