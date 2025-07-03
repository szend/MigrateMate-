namespace MigrateApi.Models
{
    public enum ColumnType
    {
        String,
        Bpchar,
        Int,
        Float,
        DateTime,
        DateTimeOffset,
        Long,
        Double,
        Boolean,
        Guid,
        Json,
        Jsonb,
        Array,
        Decimal
    }

    public class Column   
    {
        public string Name { get; set; }
        public ColumnType Type { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsArray { get; set; }
        public int MaxLenght { get; set; }
        public bool Nullabel { get; set; }

    }
}
