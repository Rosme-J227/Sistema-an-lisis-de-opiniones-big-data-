namespace CustomerOpinionsETL.Core.Interfaces;
public interface IStagingProcessor
{
    System.Threading.Tasks.Task ProcessNewAsync(System.Threading.CancellationToken ct = default);
}
