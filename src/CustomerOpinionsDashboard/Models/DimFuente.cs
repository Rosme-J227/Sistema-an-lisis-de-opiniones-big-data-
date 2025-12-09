using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace CustomerOpinionsDashboard.Models;



[Table("DimFuente", Schema = "Dimension")]
public class DimFuente
{
    [Key]
    [Column("Fuente_id")]
    public int Fuente_id { get; set; }

    [Column("Tipo_fuente")]
    [StringLength(50)]
    public string? TipoFuente { get; set; }

    [Column("Plataforma")]
    [StringLength(100)]
    public string? Plataforma { get; set; }

    [Column("Canal")]
    [StringLength(100)]
    public string? Canal { get; set; }


    public virtual ICollection<FactOpinion> Opiniones { get; set; } = new List<FactOpinion>();
}
