using System;

namespace CustomerOpinionsDashboard.Models;

public class FactOpinion
{
    public long Opinion_id { get; set; }
    public int Tiempo_id { get; set; }
    public int Producto_id { get; set; }
    public int Cliente_id { get; set; }
    public int Fuente_id { get; set; }
    public int Sentimiento_id { get; set; }
    public string? ID_Opinion_Original { get; set; }
    public string? Comentario { get; set; }
    public int? Puntaje_Satisfaccion { get; set; }
    public int? Rating { get; set; }
    public int? Longitud_Comentario { get; set; }
    public bool? Tiene_Comentario { get; set; }
    public string? Palabras_clavePositivas { get; set; }
    public string? Palabras_claveNegativas { get; set; }
    public decimal? Score_sentimiento { get; set; }
    public DateTime? Fecha_carga { get; set; }


    public virtual DimSentimiento Sentimiento { get; set; }
    public virtual DimProducto Producto { get; set; }
    public virtual DimCliente Cliente { get; set; }
    public virtual DimFuente Fuente { get; set; }
    public virtual DimTiempo Tiempo { get; set; }
}

