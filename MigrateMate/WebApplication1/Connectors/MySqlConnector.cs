using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using MigrateApi.Models;
using MySql.Data.MySqlClient;
using Npgsql;

namespace MigrateApi.Connectors
{
    public class MySqlConnector : IDbConnector
    {
        public string ConnectionString { get; set; }

        public MySqlConnector(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public async Task<List<string>> GetAllTableNameAsync()
        {
            List<string> tables = new List<string>();
            await using var myConnection = new MySqlConnection(ConnectionString);
            string query = "SHOW TABLES;";

            await myConnection.OpenAsync();
            await using var cmd = new MySqlCommand(query, myConnection);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                tables.Add(reader.GetString(0));
            }
            return tables;
        }

        public async Task<List<Table>> GetAllTableAsync(List<string> tablenames)
        {
            var tasks = tablenames.Select(async name =>
            {
                string def = await GetTableInfoAsync(name);
                return ConvertToTableDef(def);
            });

            return (await Task.WhenAll(tasks)).ToList();
        }

        public async Task<string> GetTableInfoAsync(string name)
        {
            string query = $@"
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    COLUMN_KEY,
                    IS_NULLABLE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{name}';
            ";

            string result = "";
            await using var myConnection = new MySqlConnection(ConnectionString);
            await myConnection.OpenAsync();
            await using var cmd = new MySqlCommand(query, myConnection);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                string columnName = reader.GetString("COLUMN_NAME");
                string dataType = reader.GetString("DATA_TYPE");
                object maxLenObj = reader["CHARACTER_MAXIMUM_LENGTH"];
                string columnKey = reader.GetString("COLUMN_KEY");
                string isNullable = reader.GetString("IS_NULLABLE");

                bool isPrimary = columnKey == "PRI";
                string typeMap = GetTypeMapping(dataType);
                int maxLen = maxLenObj != DBNull.Value ? Convert.ToInt32(maxLenObj) : 255;

                string line = $"{isPrimary}, {columnName}, {typeMap}, false, {maxLen}, {(isNullable == "YES" ? "true" : "false")};";
                result += line;
            }

            result = name + ":" + result;
            return result;
        }

        private string GetTypeMapping(string dataType)
        {
            switch (dataType)
            {
                case "varchar": return "String,false";
                case "char": return "Bpchar,false";
                case "int": return "Int,false";
                case "bigint": return "Long,false";
                case "double": return "Double,false";
                case "float": return "Float,false";
                case "decimal": return "Decimal,false";
                case "date": return "DateTime,false";
                case "datetime": return "DateTime,false";
                case "timestamp": return "DateTime,false";
                case "tinyint": return "Boolean,false";
                case "json": return "Json,false";
                case "binary": return "Guid,false";
                default: return "String,false";
            }
        }

        private Table ConvertToTableDef(string definition)
        {
            var parts = definition.Split(':');
            var columnsDef = parts[1].Split(';', StringSplitOptions.RemoveEmptyEntries);
            var table = new Table { Name = parts[0] };

            foreach (var colStr in columnsDef)
            {
                var colData = colStr.Split(',', StringSplitOptions.TrimEntries);
                var col = new Column
                {
                    IsPrimaryKey = bool.Parse(colData[0]),
                    Name = colData[1],
                    Type = Enum.Parse<ColumnType>(colData[2]),
                    IsArray = bool.Parse(colData[3]),
                    MaxLenght = int.Parse(colData[4]),
                    Nullabel = bool.Parse(colData[5])
                };
                table.Columns.Add(col);
            }

            return table;
        }

        public async Task<bool> CreateTableAsync(Table table)
        {
            try
            {
                await using var myConnection = new MySqlConnection(ConnectionString);
                var pkey = table.Columns.Where(x => x.IsPrimaryKey).ToList();

                string columnsSql = string.Join(",", table.Columns.Select(x =>
                    $"`{x.Name}` {TypeMapping(x)} {(x.Nullabel ? "" : "NOT NULL")}"));

                string sql = $"CREATE TABLE `{table.Name}` ({columnsSql}"
                    + (pkey.Any() ? $", PRIMARY KEY ({string.Join(",", pkey.Select(x => $"`{x.Name}`"))})" : "")
                    + ") ENGINE=InnoDB;";

                await myConnection.OpenAsync();
                await using var cmd = new MySqlCommand(sql, myConnection);
                await cmd.ExecuteNonQueryAsync();

                return true;
            }
            catch
            {
                return false;
            }
        }

        private string TypeMapping(Column col)
        {
            return col.Type switch
            {
                ColumnType.String => $"varchar({col.MaxLenght})",
                ColumnType.Bpchar => $"char({col.MaxLenght})",
                ColumnType.Int => "int",
                ColumnType.Long => "bigint",
                ColumnType.Double => "double",
                ColumnType.Float => "float",
                ColumnType.Decimal => "decimal(18,2)",
                ColumnType.DateTime => "datetime",
                ColumnType.DateTimeOffset => "datetime(6)",
                ColumnType.Boolean => "tinyint(1)",
                ColumnType.Guid => "binary(16)",
                ColumnType.Json => "json",
                ColumnType.Jsonb => "json",
                ColumnType.Array => "json",
                _ => $"varchar({col.MaxLenght})"
            };
        }


        public async Task<long> GetCountAsync(Table table)
        {
            await using var myConnection = new MySqlConnection(ConnectionString);
            string sql = $"SELECT COUNT(*) FROM `{table.Name}`;";
            await myConnection.OpenAsync();
            await using var cmd = new MySqlCommand(sql, myConnection);
            var result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt64(result);
        }

        public async Task GetDataFromTableAsync(Table table, int offset, int batchSize)
        {
            await using var myConnection = new MySqlConnection(ConnectionString);
            string orderBy = table.Columns.FirstOrDefault(x => x.IsPrimaryKey)?.Name ?? table.Columns.First().Name;
            string sql = $"SELECT * FROM `{table.Name}` ORDER BY `{orderBy}` LIMIT {batchSize} OFFSET {offset};";

            await myConnection.OpenAsync();
            await using var cmd = new MySqlCommand(sql, myConnection);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var row = new Row();
                foreach (var col in table.Columns)
                {
                    var value = reader[col.Name]?.ToString();
                    row.Cells.Add(new Cell { ColumnName = col.Name, ValueString = value });
                }
                table.Rows.Add(row);
            }
        }

        public async Task<bool> DropTableAsync(string tablename)
        {
            try
            {
                await using var myConnection = new MySqlConnection(ConnectionString);
                string sql = $"DROP TABLE IF EXISTS `{tablename}`;";
                await myConnection.OpenAsync();
                await using var cmd = new MySqlCommand(sql, myConnection);
                await cmd.ExecuteNonQueryAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public async Task GetDataFromTableAsync(Table table)
        {
            await using var myConnection = new MySqlConnection(ConnectionString);
            string sql = $"SELECT * FROM `{table.Name};";

            await myConnection.OpenAsync();
            await using var cmd = new MySqlCommand(sql, myConnection);
            await using var reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var row = new Row();
                foreach (var col in table.Columns)
                {
                    var value = reader[col.Name]?.ToString();
                    row.Cells.Add(new Cell { ColumnName = col.Name, ValueString = value });
                }
                table.Rows.Add(row);
            }
        }

        public Task<bool> InsertValueAsync(Table table, Row row)
        {
            throw new NotImplementedException();
        }

        public async Task<List<TableRelations>> GetRelationsAsync()
        {
            List<TableRelations> databaseTableRelations = new List<TableRelations>();

            using (MySqlConnection myConnection = new MySqlConnection(ConnectionString))
            {
                string query = @"
SELECT
  rc.CONSTRAINT_NAME,
  rc.TABLE_NAME AS child_table,
  kcu.COLUMN_NAME AS child_column,
  rc.REFERENCED_TABLE_NAME AS parent_table,
  rc.REFERENCED_COLUMN_NAME AS parent_column
FROM information_schema.REFERENTIAL_CONSTRAINTS AS rc
JOIN information_schema.KEY_COLUMN_USAGE AS kcu
  ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE rc.CONSTRAINT_SCHEMA = DATABASE();";

                MySqlCommand cmd = new MySqlCommand(query, myConnection);
                await myConnection.OpenAsync();

                using (MySqlDataReader reader = (MySqlDataReader)await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        TableRelations relation = new TableRelations
                        {
                            FKName = reader.GetString(0),
                            ChildTableName = reader.GetString(1),
                            ChildColName = reader.GetString(2),
                            ParentTableName = reader.GetString(3),
                            ParentColName = reader.GetString(4)
                        };
                        databaseTableRelations.Add(relation);
                    }
                }

                myConnection.Close();
            }

            return databaseTableRelations;
        }

        public async Task<bool> CreateRealtionAsync(TableRelations relation)
        {
            try
            {
                using (MySqlConnection myConnection = new MySqlConnection(ConnectionString))
                {
                    string constraintName = "FK_" + relation.FKName.Replace("~", "");
                    string sql = $@"
ALTER TABLE `{relation.ChildTableName}`
ADD CONSTRAINT `{constraintName}`
FOREIGN KEY (`{relation.ChildColName}`)
REFERENCES `{relation.ParentTableName}` (`{relation.ParentColName}`);";

                    MySqlCommand cmd = new MySqlCommand(sql, myConnection);
                    await myConnection.OpenAsync();
                    await cmd.ExecuteNonQueryAsync();
                    myConnection.Close();
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public async Task<bool> InsertValuesAsync(Table table, List<Row> rows)
        {
            try
            {
                if (rows == null || rows.Count == 0)
                    return false;

                List<string> columns = table.Columns.Select(c => "\"" + c.Name + "\"").ToList();

                List<string> valuesList = new List<string>();
                foreach (var row in rows)
                {
                    List<string> fieldValues = new List<string>();
                    foreach (var column in table.Columns)
                    {
                        var valueString = GetValueString(column, row);
                        if (valueString != "'NULL'")
                        {
                            fieldValues.Add(valueString);
                        }
                        else
                        {
                            fieldValues.Add("NULL");
                        }
                    }
                    valuesList.Add("(" + string.Join(",", fieldValues) + ")");
                }

                using (MySqlConnection myConnection = new MySqlConnection(ConnectionString))
                {
                    string oString = "INSERT INTO \"" + table.Name + "\" (" + string.Join(",", columns) + ") VALUES " + string.Join(",", valuesList);
                    using (MySqlCommand oCmd = new MySqlCommand(oString, myConnection))
                    {
                        myConnection.Open();
                        await oCmd.ExecuteNonQueryAsync();
                    }
                }

                return true;
            }
            catch (Exception e)
            {
                return false;
            }
        }

        private string GetValueString(Column field, Row row)
        {
            var res = row.Cells.FirstOrDefault(x => x.ColumnName == field.Name)?.ValueString;

            if (string.IsNullOrEmpty(res) || res == "NA")
            {
                return "NULL";
            }

            if (field.Type == ColumnType.DateTime || field.Type == ColumnType.DateTimeOffset)
            {
                try
                {
                    // Ha hosszabb a string (valószínűleg tartalmaz időt is)
                    if (res.Length > 10)
                    {
                        return $"'{res}'";
                    }
                    else
                    {
                        // Ha csak dátum, átalakítjuk yyyy-MM-dd formára (MySQL szabvány)
                        var parts = res.Split('.');
                        if (parts.Length == 3)
                        {
                            string formatted = $"{parts[2]}-{parts[1]}-{parts[0]}";
                            return $"'{formatted}'";
                        }
                        else
                        {
                            return "NULL";
                        }
                    }
                }
                catch
                {
                    return "NULL";
                }
            }
            else if (field.Type == ColumnType.Int || field.Type == ColumnType.Double || field.Type == ColumnType.Long || field.Type == ColumnType.Float)
            {
                try
                {
                    var num = double.Parse(res.Replace(',', '.'));
                    return res.Replace(',', '.');
                }
                catch
                {
                    return "NULL";
                }
            }
            else if (field.Type == ColumnType.Boolean)
            {
                // MySQL-ben BOOLEAN valójában TINYINT(1): 0 vagy 1
                if (res == "True" || res == "true" || res == "1")
                {
                    return "1";
                }
                else if (res == "False" || res == "false" || res == "0")
                {
                    return "0";
                }
                else
                {
                    return "NULL";
                }
            }
            else if (field.Type == ColumnType.Guid)
            {
                if (string.IsNullOrEmpty(res))
                {
                    return "NULL";
                }
                return $"'{res}'";
            }
            else
            {
                // Szöveg típus: aposztróf duplázás az SQL injection elkerülése miatt
                return $"'{res.Replace("'", "''")}'";
            }
        }

    }
}
