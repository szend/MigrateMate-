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
                //DbType.MySQL => new MySqlConnector(connectionString),
                //DbType.SqlServer => new SqlServerConnector(connectionString),
                //DbType.Oracle => new OracleConnector(connectionString),
                DbType.MongoDB => new MongoConnector(connectionString),
                _ => throw new NotSupportedException($"Database type {dbType} is not supported.")
            };
        }

        public void MigrateData()
        {
            var fromTableNames = _from.GetAllTableName();
            var fromTables = _from.GetAllTable(fromTableNames);

            foreach (var table in fromTables)
            {
                _to.CreateTable(table);

                InsertValues(table);
            }

            if (!_config.NoRelations)
            {
                CreateRelations();
            }

            if (_config.SaveRelations)
            {
                SaveRelations();
            }

            if (_config.DeletFromDb)
            { 
                foreach (var table in fromTables)
                {
                    _from.DropTable(table.Name);
                }
            }  
        }

        private void InsertValues(Table table)
        {
            var count = _from.GetCount(table);
            int batchSize = 1000;

            if (count > batchSize)
            {
                int totalBatches = (int)Math.Ceiling((double)count / batchSize);
                for (int i = 0; i < totalBatches; i++)
                {
                    table.Rows.Clear();
                    _from.GetDataFromTable(table, i * batchSize, batchSize);
                    _to.InsertValues(table, table.Rows);
                }
            }
            else
            {
                table.Rows.Clear();
                _from.GetDataFromTable(table);
                _to.InsertValues(table, table.Rows.ToList());
            }
        }

        private void CreateRelations()
        {
            var relations = _from.GetRelations();

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
                    _to.CreateRealtion(relation);
                }
            }

            foreach (var nn in possibleNN)
            {
                var nnRelations = relations.Where(r => r.ChildTableName == nn.Table).ToList();
                foreach (var rel in nnRelations)
                {
                    _to.CreateRealtion(rel);
                }
            }
        }

        private void SaveRelations()
        {
            Table relationTable = new Table();

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
            relationTable.Columns.Add(fkName);

            Column parentColName = new Column()
            {
                IsPrimaryKey = false,
                MaxLenght = 255,
                IsArray = false,
                Name = "ParentColName",
                Type = ColumnType.String,
                Nullabel = false,
            };
            relationTable.Columns.Add(fkName);

            Column childTableName = new Column()
            {
                IsPrimaryKey = false,
                MaxLenght = 255,
                IsArray = false,
                Name = "ChildTableName",
                Type = ColumnType.String,
                Nullabel = false,
            };
            relationTable.Columns.Add(fkName);

            Column childColName = new Column()
            {
                IsPrimaryKey = false,
                MaxLenght = 255,
                IsArray = false,
                Name = "ChildColName",
                Type = ColumnType.String,
                Nullabel = false,
            };
            relationTable.Columns.Add(fkName);

            _to.CreateTable(relationTable);

            var relations = _from.GetRelations();
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

            _to.InsertValues(relationTable, relationTable.Rows);

        }
    }
}
