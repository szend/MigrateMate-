using System.Threading.Tasks;
using MigrateApi.Models;
using MigrateApi.Request;

namespace MigrateApi.Connectors
{
    public enum DbType
    {
        PostgreSQL,
        MySQL,
        SqlServer,
        Oracle,
        MongoDB,
        JsonApi
    }

    public class DbManager
    {
        private IDbConnector _from;
        private IDbConnector _to;
        private MigrateRequestConfig _config;

        public DbManager(MigrateRequest request)
        {
            _from = GetConnector(request.FromDb.ConnectionString, request.FromDb.DbType);
            _to = GetConnector(request.ToDb.ConnectionString, request.ToDb.DbType);
            _config = request.Config;
        }

        private IDbConnector GetConnector(string connectionString, DbType dbType)
        {
            return dbType switch
            {
                DbType.PostgreSQL => new PostgreConnector(connectionString),
                DbType.MySQL => new MySqlConnector(connectionString),
                //DbType.SqlServer => new SqlServerConnector(connectionString),
                //DbType.Oracle => new OracleConnector(connectionString),
                DbType.MongoDB => new MongoConnector(connectionString),
                DbType.JsonApi => new JsonApiConnector(connectionString),
                _ => throw new NotSupportedException($"Database type {dbType} is not supported.")
            };
        }

        public async Task MigrateData()
        {
            var fromTableNames = await _from.GetAllTableNameAsync();
            var fromTables = await _from.GetAllTableAsync(fromTableNames);

            var tasklist = new List<Task>();
            foreach (var table in fromTables)
            {
                var task = Task.Run(async () =>
                {
                    await _to.CreateTableAsync(table);
                    await InsertValues(table);
                });
                tasklist.Add(task);
            }
            await Task.WhenAll(tasklist);

            if (!_config.NoRelations)
            {
                await CreateRelations();
            }

            if (_config.SaveRelations)
            {
                await SaveRelations();
            }

            if (_config.DeletFromDb)
            { 
                foreach (var table in fromTables)
                {
                    await _from.DropTableAsync(table.Name);
                }
            }  
        }

        private async Task InsertValues(Table table)
        {
            var count = await _from.GetCountAsync(table);
            if(count > _config.MaxRowCountPerTable && _config.MaxRowCountPerTable > 0)
            {
                count = _config.MaxRowCountPerTable;
            }

            if (count > _config.BatchSize)
            {
                int totalBatches = (int)Math.Ceiling((double)count / _config.BatchSize);
                for (int i = 0; i < totalBatches; i++)
                {
                    table.Rows.Clear();
                    await _from.GetDataFromTableAsync(table, i * _config.BatchSize, _config.BatchSize);
                    await _to.InsertValuesAsync(table, table.Rows);
                }
            }
            else
            {
                table.Rows.Clear();
                await _from.GetDataFromTableAsync(table);
                await _to.InsertValuesAsync(table, table.Rows.ToList());
            }
        }

        private async Task CreateRelations()
        {
            var relations = await _from.GetRelationsAsync();

            var possibleNN = relations
                .GroupBy(r => r.ChildTableName)
                .Where(g => g.Count() == 2)
                .Select(g => new
                {
                    Table = g.Key,
                    Parents = g.Select(r => r.ParentTableName).Distinct().ToList()
                })
                .Where(x => x.Parents.Count == 2)
                .ToList();

            foreach (var relation in relations)
            {
                if (!possibleNN.Any(x => x.Table == relation.ChildTableName))
                {
                    await _to.CreateRealtionAsync(relation);
                }
            }

            foreach (var nn in possibleNN)
            {
                var nnRelations = relations.Where(r => r.ChildTableName == nn.Table).ToList();
                foreach (var rel in nnRelations)
                {
                     await _to.CreateRealtionAsync(rel);
                }
            }
        }

        private async Task SaveRelations()
        {
            Table relationTable = new Table()
            {
                Name = "InternalTableRelations",
            };

            Column fkName = new Column()
            {
                IsPrimaryKey = true,
                MaxLenght = 255,
                IsArray = false,
                Name = "FKName",
                Type = ColumnType.String,
                Nullabel = false,
            };
            relationTable.Columns.Add(fkName);

            Column parentTableName = new Column()
            {
                IsPrimaryKey = false,
                MaxLenght = 255,
                IsArray = false,
                Name = "ParentTableName",
                Type = ColumnType.String,
                Nullabel = false,
            };
            relationTable.Columns.Add(parentTableName);

            Column parentColName = new Column()
            {
                IsPrimaryKey = false,
                MaxLenght = 255,
                IsArray = false,
                Name = "ParentColName",
                Type = ColumnType.String,
                Nullabel = false,
            };
            relationTable.Columns.Add(parentColName);

            Column childTableName = new Column()
            {
                IsPrimaryKey = false,
                MaxLenght = 255,
                IsArray = false,
                Name = "ChildTableName",
                Type = ColumnType.String,
                Nullabel = false,
            };
            relationTable.Columns.Add(childTableName);

            Column childColName = new Column()
            {
                IsPrimaryKey = false,
                MaxLenght = 255,
                IsArray = false,
                Name = "ChildColName",
                Type = ColumnType.String,
                Nullabel = false,
            };
            relationTable.Columns.Add(childColName);

            await _to.CreateTableAsync(relationTable);

            var relations = await _from.GetRelationsAsync();
            foreach (var relation in relations)
            {
                Row row = new Row();

                Cell fkNameCell = new Cell()
                {
                    ColumnName = "FKName",
                    ValueString = relation.FKName,
                };
                row.Cells.Add(fkNameCell);

                Cell parentTableNameCell = new Cell()
                {
                    ColumnName = "ParentTableName",
                    ValueString = relation.ParentTableName,
                };
                row.Cells.Add(parentTableNameCell);

                Cell parentColNameCell = new Cell()
                {
                    ColumnName = "ParentColName",
                    ValueString = relation.ParentColName,
                };
                row.Cells.Add(parentColNameCell);

                Cell childTableNameCell = new Cell()
                {
                    ColumnName = "ChildTableName",
                    ValueString = relation.ChildTableName,
                };
                row.Cells.Add(childTableNameCell);

                Cell childColNameCell = new Cell()
                {
                    ColumnName = "ChildColName",
                    ValueString = relation.ChildColName,
                };
                row.Cells.Add(childColNameCell);

                relationTable.Rows.Add(row);
            }

            await _to.InsertValuesAsync(relationTable, relationTable.Rows);

        }
    }
}
