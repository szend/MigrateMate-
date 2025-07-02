using MigrateApi.Models;

namespace MigrateApi.Connectors
{
    public interface IDbConnector
    {
        public List<string> GetAllTableName();
        public string GetTableInfo(string name);
        public Table ConvertToTableDef(string deffinition);
        public List<Table> GetAllTable(List<string> tablenames);
        public bool CreateTable(Table table);
        public int GetCount(Table table);
        public void GetDataFromTable(Table table);
        public void GetDataFromTable(Table table, int offset, int batchSize);
        public bool InsertValue(Table table, Row row);
        public bool InsertValues(Table table, List<Row> rows);
        public List<TableRelations> GetRelations();
        public bool CreateRealtion(TableRelations relation);
        public bool DropTable(string tablename);
    }
}
