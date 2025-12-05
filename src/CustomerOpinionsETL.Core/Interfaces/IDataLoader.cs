using CustomerOpinionsETL.Core.Entities;
namespace CustomerOpinionsETL.Core.Interfaces;
public interface IDataLoader
{
    System.Threading.Tasks.Task LoadToStagingAsync(System.Collections.Generic.IEnumerable<OpinionRaw> batch, System.Threading.CancellationToken ct = default);
}
