using Microsoft.EntityFrameworkCore;
var builder = WebApplication.CreateBuilder(args);
builder.Services.AddDbContext<SourceDbContext>(options => options.UseSqlServer(builder.Configuration.GetConnectionString("SourceOpiniones")));
var app = builder.Build();

app.MapGet("/api/reviews", async (SourceDbContext db, int page = 1, int pageSize = 100) =>
{
    var skip = (page - 1) * pageSize;
    var items = await db.Reviews.OrderBy(r => r.ReviewId).Skip(skip).Take(pageSize)
        .Select(r => new {
            r.IdOrigen, r.ClienteId, r.ProductoId, r.Fuente, r.Fecha, r.Comentario, r.Puntaje, r.Rating
        }).ToListAsync();
    return Results.Ok(new { page, pageSize, items });
});

app.Run();

public class SourceDbContext : DbContext
{
    public SourceDbContext(DbContextOptions<SourceDbContext> opts) : base(opts) { }
    public DbSet<Review> Reviews => Set<Review>();
}

public class Review
{
    public long ReviewId { get; set; }
    public string? IdOrigen { get; set; }
    public int? ClienteId { get; set; }
    public int? ProductoId { get; set; }
    public string? Fuente { get; set; }
    public DateTime Fecha { get; set; }
    public string? Comentario { get; set; }
    public int? Puntaje { get; set; }
    public int? Rating { get; set; }
}
