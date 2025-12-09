using System;

namespace CustomerOpinionsDashboard.Models;

public class DimTiempo
{
    public int Tiempo_id { get; set; }
    public DateOnly Fecha { get; set; }
    public int Dia { get; set; }
    public int Mes { get; set; }
    public int Año { get; set; }
    public int Trimestre { get; set; }
    public string? Nombre_Mes { get; set; }
    public string? Nombre_dia { get; set; }
    public string? Mes_año { get; set; }
    public bool? Es_Fin_Semana { get; set; }
}
