using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson;

namespace MigrateApi.Models
{
    public class TableRelations
    {
        public string FKName { get; set; }
        public string ParentTableName { get; set; }
        public string ParentColName { get; set; }
        public string ChildTableName { get; set; }
        public string ChildColName { get; set; }
    }
}
