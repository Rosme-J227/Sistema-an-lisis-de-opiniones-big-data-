using CustomerOpinionsETL.Core.Interfaces;
using Serilog;

namespace CustomerOpinionsETL.Infrastructure.Services;
public class SerilogLogService : ILogService
{
    private readonly ILogger _log;
    public SerilogLogService(ILogger log) => _log = log;
    public void Info(string message) => _log.Information(message);
    public void Error(string message, System.Exception? ex = null) => _log.Error(ex, message);
}
