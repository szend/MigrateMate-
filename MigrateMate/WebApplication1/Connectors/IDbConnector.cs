using MigrateApi.Models;

namespace MigrateApi.Connectors
{
    public interface IDbConnector
    {
        public Task<List<string>> GetAllTableNameAsync();
        public Task<List<Table>> GetAllTableAsync(List<string> tablenames);
        public Task<string> GetTableInfoAsync(string name);
        public Task<bool> CreateTableAsync(Table table);
        public Task<long> GetCountAsync(Table table);
        public Task GetDataFromTableAsync(Table table, int offset, int batchSize);
        public Task GetDataFromTableAsync(Table table);
        public Task<bool> InsertValueAsync(Table table, Row row);
        public  Task<List<TableRelations>> GetRelationsAsync();
        public Task<bool> CreateRealtionAsync(TableRelations relation);
        public Task<bool> DropTableAsync(string tablename);
        public Task<bool> InsertValuesAsync(Table table, List<Row> rows);

    }
}
