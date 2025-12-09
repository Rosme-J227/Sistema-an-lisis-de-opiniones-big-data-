using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CustomerOpinionsDashboard.Models;




[Table("FactTendencias", Schema = "Fact")]
public class FactTendencias
{
    [Key]
    [Column("Tendencia_id")]
    public long TendenciaId { get; set; }

    [ForeignKey("DimTiempo")]
    [Column("Tiempo_id")]
    public int TiempoId { get; set; }

    [ForeignKey("DimProducto")]
    [Column("Producto_id")]
    public int ProductoId { get; set; }

    [Column("Total_opiniones")]
    public int TotalOpiniones { get; set; }

    [Column("Promedio_satisfaccion")]
    public double? PromedioSatisfaccion { get; set; }

    [Column("Porcentaje_positivos")]
    public double? PorcentajePositivos { get; set; }

    [Column("Porcentaje_negativos")]
    public double? PorcentajeNegativos { get; set; }

    [Column("Porcentaje_neutrales")]
    public double? PorcentajeNeutrales { get; set; }

    [Column("NPS")]
    public double? NPS { get; set; }

    [Column("CSAT")]
    public double? CSAT { get; set; }

    [Column("Fecha_calculo")]
    public DateTime FechaCalculo { get; set; } = DateTime.UtcNow;


    public virtual DimTiempo? Tiempo { get; set; }
    public virtual DimProducto? Producto { get; set; }
}
