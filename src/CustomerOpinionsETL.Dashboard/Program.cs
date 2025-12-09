using System;
// (usings consolidated above)
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;

using Dapper;
using Microsoft.Data.SqlClient;
using Serilog;
using CustomerOpinionsETL.Dashboard.Services;

var builder = WebApplication.CreateBuilder(args);


Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .Enrich.FromLogContext()
    .WriteTo.Console()
    .CreateLogger();

builder.Host.UseSerilog();


builder.Services.AddControllersWithViews().AddRazorRuntimeCompilation();


builder.Services.AddScoped<IDwRepository>(sp => new DwRepository(
    builder.Configuration.GetConnectionString("DW") ?? string.Empty));

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseStaticFiles();
app.UseRouting();
app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Dashboard}/{action=Index}/{id?}");

app.Run();

