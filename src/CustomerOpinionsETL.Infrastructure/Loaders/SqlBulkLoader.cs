using System;
using CustomerOpinionsETL.Core.Entities;
using CustomerOpinionsETL.Core.Interfaces;
using Microsoft.Data.SqlClient;
using System.Data;

namespace CustomerOpinionsETL.Infrastructure.Loaders;
public class SqlBulkLoader : IDataLoader
{
    private readonly string _dwhConnection;
    private readonly int _batchSize;
    public SqlBulkLoader(string dwhConnection, int batchSize = 25000)
    {
        _dwhConnection = dwhConnection;
        _batchSize = batchSize;
    }

    public async System.Threading.Tasks.Task LoadToStagingAsync(System.Collections.Generic.IEnumerable<OpinionRaw> batch, System.Threading.CancellationToken ct = default)
    {
        var dt = new DataTable();
        dt.Columns.Add("Fuente", typeof(string));
        dt.Columns.Add("IdOrigen", typeof(string));
        dt.Columns.Add("ClienteId", typeof(int));
        dt.Columns.Add("NombreCliente", typeof(string));
        dt.Columns.Add("Email", typeof(string));
        dt.Columns.Add("CodigoProducto", typeof(string));
        dt.Columns.Add("Comentario", typeof(string));
        dt.Columns.Add("Puntaje", typeof(int));
        dt.Columns.Add("Rating", typeof(int));
        dt.Columns.Add("FechaOrigen", typeof(DateTime));

        foreach (var r in batch)
        {
            var row = dt.NewRow();
            row["Fuente"] = r.Fuente ?? (object)DBNull.Value;
            row["IdOrigen"] = r.IdOrigen ?? (object)DBNull.Value;
            row["ClienteId"] = r.ClienteId ?? (object)DBNull.Value;
            row["NombreCliente"] = r.NombreCliente ?? (object)DBNull.Value;
            row["Email"] = r.Email ?? (object)DBNull.Value;
            row["CodigoProducto"] = r.CodigoProducto ?? (object)DBNull.Value;
            row["Comentario"] = r.Comentario ?? (object)DBNull.Value;
            row["Puntaje"] = r.Puntaje ?? (object)DBNull.Value;
            row["Rating"] = r.Rating ?? (object)DBNull.Value;
            row["FechaOrigen"] = r.FechaOrigen == default ? DateTime.UtcNow : r.FechaOrigen;
            dt.Rows.Add(row);
        }

        await using var con = new SqlConnection(_dwhConnection);
        await con.OpenAsync(ct);
        using var bulk = new SqlBulkCopy(con, SqlBulkCopyOptions.TableLock, null)
        {
            DestinationTableName = "Staging.StgOpiniones",
            BatchSize = _batchSize,
            BulkCopyTimeout = 0
        };

        bulk.ColumnMappings.Add("Fuente", "Fuente");
        bulk.ColumnMappings.Add("IdOrigen", "IdOrigen");
        bulk.ColumnMappings.Add("ClienteId", "ClienteId");
        bulk.ColumnMappings.Add("NombreCliente", "NombreCliente");
        bulk.ColumnMappings.Add("Email", "Email");
        bulk.ColumnMappings.Add("CodigoProducto", "CodigoProducto");
        bulk.ColumnMappings.Add("Comentario", "Comentario");
        bulk.ColumnMappings.Add("Puntaje", "Puntaje");
        bulk.ColumnMappings.Add("Rating", "Rating");
        bulk.ColumnMappings.Add("FechaOrigen", "FechaOrigen");

        await bulk.WriteToServerAsync(dt, ct);
    }
}
