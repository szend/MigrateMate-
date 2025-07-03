using MigrateApi.Connectors;
using MigrateApi.Models;
using MongoDB.Bson.IO;
using Newtonsoft.Json;
using System.Net.Http;
using System.Threading.Tasks;

public class JsonApiConnector : IDbConnector
{
    public string ApiUrl { get; set; }

    public JsonApiConnector(string apiUrl)
    {
        ApiUrl = apiUrl;
    }

    public async Task<List<string>> GetAllTableNameAsync()
    {
        return new List<string> { ApiUrl.Split("/").Last()};
    }

    public async Task<List<Table>> GetAllTableAsync(List<string> tablenames)
    {
        var tasks = tablenames.Select(async name =>
        {
            var table = await GetTableInfoAsync(name);
            return ConvertToTableDef(name, table);
        });

        return (await Task.WhenAll(tasks)).ToList();
    }

    public async Task<string> GetTableInfoAsync(string name)
    {
        using var client = new HttpClient();
        var response = await client.GetStringAsync(ApiUrl);
        return response;
    }

    private Table ConvertToTableDef(string tableName, string json)
    {
        var table = new Table { Name = tableName };

        var items = Newtonsoft.Json.JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(json);

        if (items != null && items.Any())
        {
            var firstItem = items.First();

            foreach (var key in firstItem.Keys)
            {
                var value = firstItem[key];
                ColumnType type = ColumnType.String;

                if (value is long or int) type = ColumnType.Int;
                else if (value is double or float or decimal) type = ColumnType.Double;
                else if (value is bool) type = ColumnType.Boolean;
                else if (Guid.TryParse(value?.ToString(), out _)) type = ColumnType.Guid;

                var column = new Column
                {
                    Name = key,
                    Type = type,
                    MaxLenght = 2550,
                    IsPrimaryKey = key.Equals("id", StringComparison.OrdinalIgnoreCase),
                    Nullabel = true,
                    IsArray = false
                };
                table.Columns.Add(column);
            }

            foreach (var item in items)
            {
                Row row = new Row();
                foreach (var key in item.Keys)
                {
                    row.Cells.Add(new Cell
                    {
                        ColumnName = key,
                        ValueString = item[key]?.ToString()
                    });
                }
                table.Rows.Add(row);
            }
        }

        return table;
    }

    public async Task<long> GetCountAsync(Table table)
    {
        return table.Rows.Count;
    }

    public async Task GetDataFromTableAsync(Table table)
    {
        var json = await GetTableInfoAsync(table.Name);
        var newTable = ConvertToTableDef(table.Name, json);
        table.Rows = newTable.Rows;
    }

    public async Task GetDataFromTableAsync(Table table, int offset, int batchSize)
    {
        var json = await GetTableInfoAsync(table.Name);
        var newTable = ConvertToTableDef(table.Name, json);
        table.Rows = newTable.Rows.Skip(offset).Take(batchSize).ToList();
    }

    public async Task<bool> CreateTableAsync(Table table)
    {
        throw new NotSupportedException("JSON API nem támogat CREATE TABLE műveletet.");
    }

    public async Task<bool> InsertValueAsync(Table table, Row row)
    {
        throw new NotSupportedException("JSON API nem támogat insert műveletet.");
    }

    public async Task<bool> InsertValuesAsync(Table table, List<Row> rows)
    {
        throw new NotSupportedException("JSON API nem támogat insert műveletet.");
    }

    public async Task<bool> DropTableAsync(string tablename)
    {
        throw new NotSupportedException("JSON API nem támogat drop table műveletet.");
    }

    public async Task<List<TableRelations>> GetRelationsAsync()
    {
        return new List<TableRelations>();
    }

    public async Task<bool> CreateRealtionAsync(TableRelations relation)
    {
        throw new NotSupportedException("JSON API nem támogat relációkat.");
    }
}
