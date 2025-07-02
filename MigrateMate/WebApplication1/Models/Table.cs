namespace MigrateApi.Models
{
    public class Table
    {
        public string? Name { get; set; }

        public virtual List<Column> Columns { get; set; } = new List<Column>();

        public virtual List<Row> Rows { get; set; } = new List<Row>();

    }
}
