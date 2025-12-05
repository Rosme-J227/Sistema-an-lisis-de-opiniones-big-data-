using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CustomerOpinionsETL.Core.Interfaces;
using CustomerOpinionsETL.Core.Entities;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks.Dataflow;

public class WorkerHosted : BackgroundService
{
    private readonly ILogger<WorkerHosted> _logger;
    private readonly IEnumerable<IExtractor> _extractors;
    private readonly IDataLoader _loader;
    private readonly CustomerOpinionsETL.Core.Interfaces.IStagingProcessor? _stagingProcessor;
    private readonly IConfiguration _cfg;

    public WorkerHosted(ILogger<WorkerHosted> logger, IEnumerable<IExtractor> extractors, IDataLoader loader, IConfiguration cfg, CustomerOpinionsETL.Core.Interfaces.IStagingProcessor? stagingProcessor = null)
    {
        _logger = logger;
        _extractors = extractors;
        _loader = loader;
        _cfg = cfg;
        _stagingProcessor = stagingProcessor;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker started at {time}", DateTimeOffset.Now);

        var buffer = new BufferBlock<OpinionRaw>(new DataflowBlockOptions { BoundedCapacity = 100000 });


        var producers = _extractors.Select(ext => Task.Run(async () =>
        {
            try
            {
                var items = await ext.ExtractAsync(stoppingToken);
                foreach (var item in items)
                {
                    await buffer.SendAsync(item, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Extractor failed");
            }
        }, stoppingToken)).ToList();


        var batchSize = int.TryParse(_cfg["BatchSize"], out var bs) ? bs : 25000;
        var consumer = Task.Run(async () =>
        {
            var batch = new List<OpinionRaw>(batchSize);
            while (!stoppingToken.IsCancellationRequested)
            {
                OpinionRaw? item = null;
                try
                {
                    item = await buffer.ReceiveAsync(stoppingToken);
                }
                catch (OperationCanceledException) { break; }
                if (item != null)
                {
                    batch.Add(item);
                    if (batch.Count >= batchSize)
                    {
                        await _loader.LoadToStagingAsync(batch, stoppingToken);
                        _logger.LogInformation("Loaded batch {count}", batch.Count);

                        if (_stagingProcessor != null)
                        {
                            try
                            {
                                await _stagingProcessor.ProcessNewAsync(stoppingToken);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Staging processing failed");
                            }
                        }
                        batch.Clear();
                    }
                }
            }
            if (batch.Any())
            {
                await _loader.LoadToStagingAsync(batch, stoppingToken);
                _logger.LogInformation("Final flush loaded {count}", batch.Count);
                if (_stagingProcessor != null)
                {
                    try
                    {
                        await _stagingProcessor.ProcessNewAsync(stoppingToken);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Staging processing failed");
                    }
                }
            }
        }, stoppingToken);

        await Task.WhenAll(producers.Append(consumer));
        _logger.LogInformation("Worker finished at {time}", DateTimeOffset.Now);
    }
}
