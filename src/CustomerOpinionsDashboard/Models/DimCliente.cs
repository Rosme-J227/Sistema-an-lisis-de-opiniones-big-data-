namespace CustomerOpinionsDashboard.Models;

public class DimCliente
{
    public int Cliente_id { get; set; }
    public int? Id_Cliente_origen { get; set; }
    public string? Nombre_Cliente { get; set; }
    public string? Email { get; set; }
    public string? Segmento { get; set; }
    public string? Pais { get; set; }
    public string? Rango_Edad { get; set; }
}
