namespace CustomerOpinionsDashboard.Models;

public class DimSentimiento
{
    public int Sentimiento_id { get; set; }
    public string? Clasificacion { get; set; }
    public int Puntuacion_min { get; set; }
    public int Puntuacion_max { get; set; }
    public string? Color { get; set; }
}
