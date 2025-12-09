using System;

namespace CustomerOpinionsDashboard.Models;

public class DimProducto
{
    public int Producto_id { get; set; }
    public int? Id_Producto_Origen { get; set; }
    public string Nombre_producto { get; set; } = string.Empty;
    public string? Categoria { get; set; }
    public DateOnly? Fecha_alta { get; set; }
    public bool? activo { get; set; }
}
