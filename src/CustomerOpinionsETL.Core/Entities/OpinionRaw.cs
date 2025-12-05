namespace CustomerOpinionsETL.Core.Entities;
public record OpinionRaw
{
    public string? Fuente { get; init; }
    public string? IdOrigen { get; init; }
    public int? ClienteId { get; init; }
    public string? NombreCliente { get; init; }
    public string? Email { get; init; }
    public string? CodigoProducto { get; init; }
    public string? Comentario { get; init; }
    public int? Puntaje { get; init; }
    public int? Rating { get; init; }
    public System.DateTime FechaOrigen { get; set; } = System.DateTime.UtcNow;
}
