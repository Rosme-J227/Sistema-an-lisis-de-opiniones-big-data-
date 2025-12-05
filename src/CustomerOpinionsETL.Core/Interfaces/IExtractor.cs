using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CustomerOpinionsETL.Core.Entities;
namespace CustomerOpinionsETL.Core.Interfaces;
public interface IExtractor
{
    Task<List<OpinionRaw>> ExtractAsync(CancellationToken ct = default);
}
