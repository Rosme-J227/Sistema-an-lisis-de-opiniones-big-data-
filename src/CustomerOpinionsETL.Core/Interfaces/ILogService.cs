namespace CustomerOpinionsETL.Core.Interfaces;
public interface ILogService
{
    void Info(string message);
    void Error(string message, System.Exception? ex = null);
}
