using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Serilog;
using CustomerOpinionsETL.Core.Interfaces;
using CustomerOpinionsETL.Infrastructure.Extractors;
using CustomerOpinionsETL.Infrastructure.Loaders;
using CustomerOpinionsETL.Infrastructure.Services;

var builder = Host.CreateDefaultBuilder(args)
    .UseSerilog((ctx, lc) => lc.WriteTo.File("logs/worker.log", rollingInterval: Serilog.RollingInterval.Day))
    .ConfigureServices((context, services) =>
    {
        var cfg = context.Configuration;

        services.AddSingleton<IExtractor>(sp => new CsvExtractor(cfg["Csv:FilePath"] ?? "data/encuestas.csv"));
        services.AddSingleton<IExtractor>(sp => new DatabaseExtractor(cfg.GetConnectionString("SourceOpiniones") ?? ""));
        services.AddHttpClient("ApiExtractor")
              .ConfigureHttpClient((sp, client) => client.BaseAddress = new Uri(cfg["ApiSource:BaseUrl"] ?? "http://localhost:5001"));
        services.AddSingleton<IExtractor>(sp => new ApiExtractor(sp.GetRequiredService<System.Net.Http.IHttpClientFactory>().CreateClient("ApiExtractor")));

        services.AddSingleton<IDataLoader>(sp => new SqlBulkLoader(cfg.GetConnectionString("DW") ?? "", cfg.GetValue<int>("BatchSize")));


        services.AddSingleton<ILogService>(sp => new SerilogLogService(Log.Logger));


        services.AddSingleton<ISentimentAnalyzer, CustomerOpinionsETL.Infrastructure.Sentiment.SimpleSentimentAnalyzer>();

        services.AddSingleton<IStagingProcessor>(sp => new CustomerOpinionsETL.Infrastructure.Loaders.StagingProcessor(
            cfg.GetConnectionString("DW") ?? "",
            sp.GetRequiredService<ISentimentAnalyzer>(),
            sp.GetRequiredService<ILogService>()));

        services.AddHostedService(sp => new WorkerHosted(
            sp.GetRequiredService<ILogger<WorkerHosted>>(),
            sp.GetServices<IExtractor>(),
            sp.GetRequiredService<IDataLoader>(),
            sp.GetRequiredService<IConfiguration>(),
            sp.GetRequiredService<IStagingProcessor>()));
    });


var host = builder.Build();

using (var scope = host.Services.CreateScope())
{
    var proc = scope.ServiceProvider.GetService<CustomerOpinionsETL.Core.Interfaces.IStagingProcessor>();
    if (proc != null)
    {

        await proc.ProcessNewAsync();
    }
}

await host.RunAsync();
