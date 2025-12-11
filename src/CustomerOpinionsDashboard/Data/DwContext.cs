using Microsoft.EntityFrameworkCore;
using CustomerOpinionsDashboard.Models;

namespace CustomerOpinionsDashboard.Data;

public class DwContext : DbContext
{
    public DwContext(DbContextOptions<DwContext> options) : base(options) { }

    public DbSet<DimCliente> DimClientes { get; set; } = null!;
    public DbSet<DimProducto> DimProductos { get; set; } = null!;
    public DbSet<DimSentimiento> DimSentimientos { get; set; } = null!;
    public DbSet<DimTiempo> DimTiempos { get; set; } = null!;
    public DbSet<FactOpinion> FactOpiniones { get; set; } = null!;
    public DbSet<DimFuente> DimFuentes { get; set; } = null!;
    public DbSet<FactTendencias> FactTendencias { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<DimCliente>(b =>
        {
            b.ToTable("DimCliente", "Dimension");
            b.HasKey(x => x.Cliente_id);
        });

        modelBuilder.Entity<DimProducto>(b =>
        {
            b.ToTable("DimProducto", "Dimension");
            b.HasKey(x => x.Producto_id);
        });

        modelBuilder.Entity<DimSentimiento>(b =>
        {
            b.ToTable("DimSentimiento", "Dimension");
            b.HasKey(x => x.Sentimiento_id);
        });

        modelBuilder.Entity<DimTiempo>(b =>
        {
            b.ToTable("DimTiempo", "Dimension");
            b.HasKey(x => x.Tiempo_id);
        });

        modelBuilder.Entity<DimFuente>(b =>
        {
            b.ToTable("DimFuente", "Dimension");
            b.HasKey(x => x.Fuente_id);
        });

        modelBuilder.Entity<FactOpinion>(b =>
        {
            b.ToTable("FactOpiniones", "Fact");
            b.HasKey(x => x.Opinion_id);
        });

        modelBuilder.Entity<FactTendencias>(b =>
        {
            b.ToTable("FactTendencias", "Fact");
            b.HasKey(x => x.TendenciaId);
        });

        base.OnModelCreating(modelBuilder);
    }
}
