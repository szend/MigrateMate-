using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MigrateApi.Models;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MigrateApi.Connectors
{
    public class MongoConnector : IDbConnector
    {
        public string ConnectionString { get; set; }
        private readonly IMongoClient _client;
        private readonly IMongoDatabase _database;

        public MongoConnector(string connectionString)
        {
            ConnectionString = connectionString;
            _client = new MongoClient(ConnectionString);

            // Kinyerjük az adatbázis nevét a connection stringből:
            var dbName = MongoUrl.Create(connectionString).DatabaseName;
            if (string.IsNullOrEmpty(dbName))
                throw new Exception("Database name must be specified in connection string.");

            _database = _client.GetDatabase(dbName);
        }

        public async Task<List<string>> GetAllTableNameAsync()
        {
            var list = await _database.ListCollectionNamesAsync();
            return await list.ToListAsync();
        }


        public async Task<List<Table>> GetAllTableAsync(List<string> tablenames)
        {

            List<Table> types = new List<Table>();
            int idx = 0;
            foreach (var item in tablenames)
            {
                string resultClassString = await GetTableInfoAsync(item);
                types.Add(ConvertToTableDef(resultClassString));
                idx++;
            }

            return types.Where(x => x.Name != "InternalTableRelations").ToList();
        }

        private Table ConvertToTableDef(string deffinition)
        {
            var nameValuePair = deffinition.Split(':');
            var columnsString = nameValuePair[1].Split(";");

            Table table = new Table();
            table.Name = nameValuePair[0];

            foreach (var columnStr in columnsString)
            {
                if (columnStr.Trim() != string.Empty)
                {
                    var coldata = columnStr.Split(',');
                    Column column = new Column();
                    column.IsPrimaryKey = bool.Parse(coldata[0]);
                    column.Name = coldata[1];
                    column.Type = Enum.Parse<ColumnType>(coldata[2]);
                    column.IsArray = bool.Parse(coldata[3]);
                    column.MaxLenght = int.Parse(coldata[4]);
                    column.Nullabel = bool.Parse(coldata[5]);
                    table.Columns.Add(column);
                }
            }

            return table;
        }

        public async Task<string> GetTableInfoAsync(string name)
        {
            var collection = _database.GetCollection<BsonDocument>(name);

            var sampleDocs = await collection.Find(new BsonDocument()).Limit(100).ToListAsync();

            var fieldTypes = new Dictionary<string, HashSet<string>>();
            var fieldNullable = new Dictionary<string, bool>();

            foreach (var doc in sampleDocs)
            {
                foreach (var element in doc.Elements)
                {
                    var fieldName = element.Name;
                    var fieldType = element.Value.BsonType.ToString();

                    if (!fieldTypes.ContainsKey(fieldName))
                        fieldTypes[fieldName] = new HashSet<string>();

                    fieldTypes[fieldName].Add(fieldType);
                }

                foreach (var field in fieldTypes.Keys)
                {
                    if (!doc.Contains(field))
                    {
                        fieldNullable[field] = true;
                    }
                }
            }

            foreach (var field in fieldTypes.Keys)
            {
                if (!fieldNullable.ContainsKey(field))
                    fieldNullable[field] = false;
            }

            var sb = new StringBuilder();

            foreach (var kvp in fieldTypes)
            {
                string field = kvp.Key;
                var types = kvp.Value;

                var firstType = types.First();

                string targetType = firstType switch
                {
                    "Int32" => "Int,false,",
                    "Int64" => "Long,false,",
                    "Double" => "Double,false,",
                    "Decimal128" => "Decimal,false,",
                    "Boolean" => "Boolean,false,",
                    "String" => "String,false,",
                    "ObjectId" => "Guid,false,",
                    "DateTime" => "DateTime,false,",
                    "Array" => "Jsonb,true,",
                    _ => "String,false,"
                };

                string isPrimaryKey = (field == "_id") ? "true," : "false,";

                string maxLength = firstType == "String" ? "555," : "0,";

                string nullable = fieldNullable[field] ? "true;" : "false;";

                sb.Append($"{isPrimaryKey}{field},{targetType}{maxLength}{nullable} ");
            }

            string result = name + ":" + sb.ToString().Trim();
            return result;
        }


        public async Task<bool> CreateTableAsync(Table table)
        {
            try
            {
                await _database.CreateCollectionAsync(table.Name);
                return true;
            }
            catch
            {
                return false;
            }
        }

        public async Task<long> GetCountAsync(Table table)
        {
            var collection = _database.GetCollection<BsonDocument>(table.Name);
            return await collection.CountDocumentsAsync(new BsonDocument());
        }

        public async Task GetDataFromTableAsync(Table table, int offset, int batchSize)
        {
            var collection = _database.GetCollection<BsonDocument>(table.Name);

            var documents = await collection.Find(new BsonDocument())
                                      .Skip(offset)
                                      .Limit(batchSize)
                                      .ToListAsync();

            foreach (var doc in documents)
            {
                var row = new Row();
                foreach (var col in table.Columns)
                {
                    var value = doc.GetValue(col.Name, string.Empty)?.ToString();
                    row.Cells.Add(new Cell
                    {
                        ColumnName = col.Name,
                        ValueString = value
                    });
                }
                table.Rows.Add(row);
            }
        }

        public async Task GetDataFromTableAsync(Table table)
        {
            var collection = _database.GetCollection<BsonDocument>(table.Name);
            var documents = await collection.Find(new BsonDocument()).ToListAsync();

            foreach (var doc in documents)
            {
                var row = new Row();
                foreach (var col in table.Columns)
                {
                    var value = doc.GetValue(col.Name, string.Empty)?.ToString();
                    row.Cells.Add(new Cell
                    {
                        ColumnName = col.Name,
                        ValueString = value
                    });
                }
                table.Rows.Add(row);
            }
        }

        public async Task<bool> InsertValueAsync(Table table, Row row)
        {
            var doc = new BsonDocument();

            foreach (var cell in row.Cells)
            {
                // ha ez a kulcs oszlop, tegyük be _id-ként is
                if (cell.ColumnName == table.Columns.First(x => x.IsPrimaryKey == true).Name)
                {
                    doc["_id"] = cell.ValueString != null ? (BsonValue)cell.ValueString : BsonNull.Value;
                }

                doc[cell.ColumnName] = cell.ValueString != null ? (BsonValue)cell.ValueString : BsonNull.Value;
            }

            var collection = _database.GetCollection<BsonDocument>(table.Name);
            await collection.InsertOneAsync(doc);
            return true;
        }


        public async Task<List<TableRelations>> GetRelationsAsync()
        {
            var collection = _database.GetCollection<BsonDocument>("InternalTableRelations");
            var docs = await collection.Find(new BsonDocument()).ToListAsync();

            var relations = docs.Select(doc =>
            {
                return new TableRelations
                {
                    FKName = doc.GetValue("FKName", "").AsString,
                    ParentTableName = doc.GetValue("ParentTableName", "").AsString,
                    ChildTableName = doc.GetValue("ChildTableName", "").AsString,
                    ParentColName = doc.GetValue("ParentColName", "").AsString,
                    ChildColName = doc.GetValue("ChildColName", "").AsString,
                };
            }).ToList();

            return relations;
        }

        public async Task<bool> CreateRealtionAsync(TableRelations relation)
        {
            try
            {
                var parentCollection = _database.GetCollection<BsonDocument>(relation.ParentTableName);
                var childCollection = _database.GetCollection<BsonDocument>(relation.ChildTableName);

                // Lekérjük az összes parent dokumentumot
                var parentDocs = await parentCollection.Find(new BsonDocument()).ToListAsync();

                foreach (var parent in parentDocs)
                {
                    var parentKey = parent.GetValue(relation.ParentColName);

                    // Lekérjük az összes child dokumentumot, ami ehhez a parent-hez tartozik
                    var filter = Builders<BsonDocument>.Filter.Eq(relation.ChildColName, parentKey);
                    var childDocs = childCollection.Find(filter).ToList();

                    if (childDocs.Any())
                    {
                        // Embed-elés: hozzáadunk egy új mezőt a parent dokumentumhoz, pl. "Children"
                        var update = Builders<BsonDocument>.Update.Set(relation.ChildTableName, new BsonArray(childDocs));

                        parentCollection.UpdateOne(
                            Builders<BsonDocument>.Filter.Eq(relation.ParentColName, parentKey),
                            update
                        );
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }


        public async Task<bool> DropTableAsync(string tablename)
        {
            await _database.DropCollectionAsync(tablename);
            return true;
        }

        private string GetValueString(Column field, Row row)
        {
            var res = row.Cells.FirstOrDefault(x => x.ColumnName == field.Name)?.ValueString;
            return res ?? string.Empty;
        }

        public async Task<bool> InsertValuesAsync(Table table, List<Row> rows)
        {
            if (rows == null || rows.Count == 0)
                return false;

            var docs = new List<BsonDocument>();

            foreach (var row in rows)
            {
                var doc = new BsonDocument();

                foreach (var cell in row.Cells)
                {
                    // ha ez a kulcs oszlop, tegyük be _id-ként is
                    if (cell.ColumnName == table.Columns.FirstOrDefault(x => x.IsPrimaryKey)?.Name)
                    {
                        doc["_id"] = cell.ValueString != null ? (BsonValue)cell.ValueString : BsonNull.Value;
                    }

                    doc[cell.ColumnName] = cell.ValueString != null ? (BsonValue)cell.ValueString : BsonNull.Value;
                }

                docs.Add(doc);
            }

            var collection = _database.GetCollection<BsonDocument>(table.Name);
            await collection.InsertManyAsync(docs);

            return true;
        }

    }
}
