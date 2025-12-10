using System;
using System.Linq;
using Microsoft.Extensions.Configuration;
using CustomerOpinionsETL.Core.Interfaces;

namespace CustomerOpinionsETL.Infrastructure.Sentiment;
public class SimpleSentimentAnalyzer : ISentimentAnalyzer
{
    private readonly string[] _positiveWords;
    private readonly string[] _negativeWords;

    public SimpleSentimentAnalyzer(IConfiguration config)
    {
        var pos = config["Sentiment:PositiveWords"];
        var neg = config["Sentiment:NegativeWords"];

        if (!string.IsNullOrWhiteSpace(pos))
            _positiveWords = pos.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim().ToLowerInvariant()).ToArray();
        else
            _positiveWords = new[] { "bueno", "excelente", "genial", "perfecto", "agradable", "recomend", "love", "great", "fantast" };

        if (!string.IsNullOrWhiteSpace(neg))
            _negativeWords = neg.Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim().ToLowerInvariant()).ToArray();
        else
            _negativeWords = new[] { "malo", "terrible", "horrible", "defecto", "devolver", "hate", "bad", "peor", "problema" };
    }

    public (string Classification, double Score) Analyze(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return ("Neutral", 0.0);

        var lower = text.ToLowerInvariant();
        int pos = _positiveWords.Count(w => lower.Contains(w));
        int neg = _negativeWords.Count(w => lower.Contains(w));

        if (pos == 0 && neg == 0)
            return ("Neutral", 0.0);

        var score = (pos - neg) / (double)(pos + neg);
        var cls = score > 0 ? "Positive" : score < 0 ? "Negative" : "Neutral";
        return (cls, Math.Round(score, 2));
    }
}
