using System.Collections.Generic;
using System;
using System.Security.Cryptography.Xml;
using MigrateApi.Models;
using Npgsql;
using System.Threading.Tasks;

namespace MigrateApi.Connectors
{
    public class PostgreConnector : IDbConnector
    {
        public string ConnectionString { get; set; }

        public PostgreConnector(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public async Task<List<string>> GetAllTableNameAsync()
        {
            List<string> tables = new List<string>();
            await using var myConnection = new NpgsqlConnection(ConnectionString);
            string oString = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';";

            await myConnection.OpenAsync();
            await using var oCmd = new NpgsqlCommand(oString, myConnection);
            await using var oReader = await oCmd.ExecuteReaderAsync();

            while (await oReader.ReadAsync())
            {
                string classname = oReader[0]?.ToString();
                if (!string.IsNullOrEmpty(classname))
                {
                    tables.Add(classname);
                }
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
            string? oString = @"
                    SELECT 
                    case 
                    when column_name = (SELECT c.column_name 
                    FROM information_schema.table_constraints tc 
                    JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name)  
                    JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema 
                      AND tc.table_name = c.table_name AND ccu.column_name = c.column_name 
                    WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = '" + name + @"') THEN 'true,'
                        else 'false,'
 end 
 ||
'" + "' || column_name || " + " '" + @",'
||
                      case
                        when data_type = 'uuid' THEN 'Guid,false,'
                        WHEN data_type = 'character' THEN 'Bpchar,false,'
                        when data_type = 'character varying' THEN 'String,false,'
                        when data_type = 'text' then 'String,false,'
                        when data_type = 'json' then 'String,false,'
                        when data_type = 'integer' then 'Int,false,'
                        when data_type = 'boolean' then 'Boolean,false,'
                        when data_type = 'numeric' then 'Double,false,'
                        when data_type = 'date' then 'DateTime,false,'
                        when data_type = 'bigint' then 'Long,false,'
                        WHEN data_type = 'json' THEN 'Json,false,'
                        WHEN data_type = 'jsonb' THEN 'Jsonb,false,'
                        when data_type = 'timestamp with time zone' then 'DateTimeOffset,false,'
                        when data_type = 'timestamptz' then 'DateTime,false,'
                        when data_type = 'timestamp without time zone' then 'DateTime,false,'
                            when data_type = 'money' then 'Decimal,false,'
                        when data_type = 'numeric' then 'Decimal,false,'
                          when data_type = 'real' then 'Float,false,'
                        when data_type = 'ARRAY' then
                            (case when udt_name = '_text' then 'String'
                                when udt_name = '_uuid' then 'Guid'
                                when udt_name = '_int4' then 'Int'
                             else data_type end)
                                                                                         || 'true,'
                        else 'String,false,'
                        end
|| COALESCE(character_maximum_length || ',', '255,')
|| CASE WHEN is_nullable = 'YES' THEN 'true;' ELSE 'false;' END
                      as sql 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                      AND table_name   = '" + name + @"';
                                    ";
            string result = "";
            await using var myConnection = new NpgsqlConnection(ConnectionString);
            await myConnection.OpenAsync();
            await using var oCmd = new NpgsqlCommand(oString, myConnection);
            await using var oReader = await oCmd.ExecuteReaderAsync();

            while (await oReader.ReadAsync())
            {
                for (int i = 0; i < oReader.FieldCount; i++)
                {
                    result += " " + oReader[i]?.ToString() + " ";
                }
            }

            result = name + ":" + result;
            return result;
        }
        private Table ConvertToTableDef(string deffinition)
        {
            var nameValuePair = deffinition.Split(':');
            var columnsString = nameValuePair[1].Split(";");

            Table table = new Table();
            table.Name = nameValuePair[0];

            foreach (var columnStr in columnsString)
            {
                if(columnStr.Trim() != string.Empty)
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

        public async Task<bool> CreateTableAsync(Table table)
        {
            try
            {
                await using var myConnection = new NpgsqlConnection(ConnectionString);
                var pkey = table.Columns.Where(x => x.IsPrimaryKey).ToList();

                string columnsSql = string.Join(",", table.Columns.Select(x =>
                    "\"" + x.Name + "\" " + TypeMapping(x) + (x.Nullabel ? "" : " NOT NULL")));

                string oString = "CREATE TABLE \"" + table.Name + "\" (" +
                                 columnsSql +
                                 (pkey.Any() ? ", PRIMARY KEY (" + string.Join(",", pkey.Select(x => "\"" + x.Name + "\"")) + ")" : "") +
                                 ")";

                await myConnection.OpenAsync();
                await using var oCmd = new NpgsqlCommand(oString, myConnection);
                await oCmd.ExecuteNonQueryAsync();
            }
            catch (Exception)
            {
                return false;
            }
            return true;
        }

        private string TypeMapping(Column col)
        {
            if (col.Type == ColumnType.String)
            {
                return "varchar(" + col.MaxLenght + ")";
            }
            else if (col.Type == ColumnType.Bpchar)
            {
                return "bpchar(" + col.MaxLenght + ")";
            }
            else if (col.Type == ColumnType.Int || col.Type == ColumnType.Long || col.Type == ColumnType.Double || col.Type == ColumnType.Float)
            {
                return "numeric";
            }
            else if (col.Type == ColumnType.DateTime)
            {
                return "date";
            }
            else if (col.Type == ColumnType.DateTimeOffset)
            {
                return "timestamptz";
            }
            else if (col.Type == ColumnType.Boolean)
            {
                return "bool";
            }
            else if (col.Type == ColumnType.Jsonb)
            {
                return "jsonb";
            }
            else if (col.Type == ColumnType.Json)
            {
                return "json";
            }
            else if (col.Type == ColumnType.Guid)
            {
                return "uuid";
            }
            else
            {
                return "varchar(" + col.MaxLenght + ")";
            }
        }

        public async Task<long> GetCountAsync(Table table)
        {
            await using var myConnection = new NpgsqlConnection(ConnectionString);
            string oString = $"SELECT COUNT(*) FROM \"{table.Name}\"";

            await myConnection.OpenAsync();
            await using var oCmd = new NpgsqlCommand(oString, myConnection);
            object result = await oCmd.ExecuteScalarAsync();

            return Convert.ToInt64(result);
        }


        public async Task GetDataFromTableAsync(Table table, int offset, int batchSize)
        {
            await using var myConnection = new NpgsqlConnection(ConnectionString);
            string orderBy = table.Columns.FirstOrDefault(x => x.IsPrimaryKey)?.Name ?? table.Columns.First().Name;
            string oString = $"SELECT * FROM \"{table.Name}\" ORDER BY \"{orderBy}\" LIMIT {batchSize} OFFSET {offset}";

            await myConnection.OpenAsync();
            await using var oCmd = new NpgsqlCommand(oString, myConnection);
            await using var oReader = await oCmd.ExecuteReaderAsync();

            while (await oReader.ReadAsync())
            {
                Row row = new Row();
                foreach (var item in table.Columns)
                {
                    var fieldvalue = oReader[item.Name];
                    row.Cells.Add(new Cell
                    {
                        ColumnName = item.Name,
                        ValueString = fieldvalue?.ToString()
                    });
                }
                table.Rows.Add(row);
            }
        }


        public async Task GetDataFromTableAsync(Table table)
        {
            await using var myConnection = new NpgsqlConnection(ConnectionString);
            string orderBy = table.Columns.FirstOrDefault(x => x.IsPrimaryKey)?.Name ?? table.Columns.First().Name;
            string oString = $"SELECT * FROM \"{table.Name}\"";

            await myConnection.OpenAsync();
            await using var oCmd = new NpgsqlCommand(oString, myConnection);
            await using var oReader = await oCmd.ExecuteReaderAsync();

            while (await oReader.ReadAsync())
            {
                Row row = new Row();
                foreach (var item in table.Columns)
                {
                    var fieldvalue = oReader[item.Name];
                    row.Cells.Add(new Cell
                    {
                        ColumnName = item.Name,
                        ValueString = fieldvalue?.ToString()
                    });
                }
                table.Rows.Add(row);
            }
        }

        public async Task<bool> InsertValueAsync(Table table, Row row)
        {
            Dictionary<string, string> fieldvalues = new Dictionary<string, string>();
            foreach (var item in table.Columns)
            {
                var valuesstring = GetValueString(item, row);
                if (valuesstring != "'NULL'")
                {
                    fieldvalues.Add("\"" + item.Name + "\"", valuesstring);
                }
            }
            using (NpgsqlConnection myConnection = new NpgsqlConnection(ConnectionString))
            {
                string oString = "INSERT INTO \"" + table.Name + "\" (" + string.Join(",", fieldvalues.Keys) + ") VALUES (" + string.Join(",", fieldvalues.Values) + ")";
                NpgsqlCommand oCmd = new NpgsqlCommand(oString, myConnection);
                myConnection.Open();
                await oCmd.ExecuteNonQueryAsync();
                myConnection.Close();
            }
            return true;

        }

        public async Task<List<TableRelations>> GetRelationsAsync()
        {
            List<TableRelations> databaseTableRelations = new List<TableRelations>();
            using (NpgsqlConnection myConnection = new NpgsqlConnection(ConnectionString))
            {
                string oString = @"
SELECT
tc.constraint_name, tc.table_name, kcu.column_name, 
ccu.table_name AS foreign_table_name,
ccu.column_name AS foreign_column_name 
FROM 
information_schema.table_constraints AS tc 
JOIN information_schema.key_column_usage AS kcu
  ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu
  ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY'
";
                NpgsqlCommand oCmd = new NpgsqlCommand(oString, myConnection);
                myConnection.Open();
                using (NpgsqlDataReader oReader = await oCmd.ExecuteReaderAsync())
                {
                    while (oReader.Read())
                    {
                        TableRelations databaseTableRelation = new TableRelations();
                        databaseTableRelation.FKName = oReader.GetString(0);
                        databaseTableRelation.ParentTableName = oReader.GetString(3);
                        databaseTableRelation.ParentColName = oReader.GetString(4);
                        databaseTableRelation.ChildTableName = oReader.GetString(1);
                        databaseTableRelation.ChildColName = oReader.GetString(2);
                        databaseTableRelations.Add(databaseTableRelation);
                    }

                    myConnection.Close();
                }
            }

            return databaseTableRelations;
        }

        public async Task<bool> CreateRealtionAsync(TableRelations relation)
        {
            try
            {
                using (NpgsqlConnection myConnection = new NpgsqlConnection(ConnectionString))
                {
                    string oString = "ALTER TABLE \"" + relation.ChildTableName + "\" ADD CONSTRAINT FK_" + relation.FKName.Replace("~", "") + " FOREIGN KEY (\"" + relation.ChildColName + "\") REFERENCES \"" + relation.ParentTableName + "\" (\"" + relation.ParentColName + "\")";
                    NpgsqlCommand oCmd = new NpgsqlCommand(oString, myConnection);
                    myConnection.Open();
                    await oCmd.ExecuteNonQueryAsync();
                    myConnection.Close();
                }

                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        public async Task<bool> DropTableAsync(string tablename)
        {
            try
            {
                using (NpgsqlConnection myConnection = new NpgsqlConnection(ConnectionString))
                {
                    string oString = "DROP TABLE IF EXISTS \"" + tablename + "\" CASCADE";
                    using (NpgsqlCommand oCmd = new NpgsqlCommand(oString, myConnection))
                    {
                        myConnection.Open();
                        await oCmd.ExecuteNonQueryAsync();
                    }
                }
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private string GetValueString(Column field,Row row)
        {
            var res = row.Cells.FirstOrDefault(x => x.ColumnName == field.Name).ValueString;
            if (res == null)
            {
                return "'NULL'";
            }
            if (field.Type == ColumnType.DateTime || field.Type == ColumnType.DateTimeOffset)
            {
                if (res == "NA" || res == string.Empty)
                {
                    return "'NULL'";
                }
                try
                {
                    if (res.Length > 10)
                    {
                        var splitted = res.Split(' ');
                        if(field.Type == ColumnType.DateTime)
                        {
                            return $"'{res}'";
                        }
                        else
                        {
                            return $"'{res}'";
                        }
                    }
                    else
                    {
                        return $"'{res?.Split('.')[2] + "." + res.Split('.')[1] + "." + res.Split('.')[0]}'";
                    }
                }
                catch (Exception)
                {
                    return "'NULL'";
                }
            }
            else if (field.Type == ColumnType.Int || field.Type == ColumnType.Double || field.Type == ColumnType.Long || field.Type == ColumnType.Float)
            {
                try
                {
                    var num = double.Parse(res ?? "0");
                }
                catch (Exception)
                {

                    return "'NULL'";
                }
                return res?.Replace(",", ".");
            }
            else if (field.Type == ColumnType.Boolean)
            {
                return $"{res}";
            }
            else if (field.Type == ColumnType.Guid)
            {
                if(res == string.Empty)
                {
                    return "'NULL'";
                }
                return $"'{res}'";
            }
            else
            {
                return $"'{res?.Replace("'", "''")}'";
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

                using (NpgsqlConnection myConnection = new NpgsqlConnection(ConnectionString))
                {
                    string oString = "INSERT INTO \"" + table.Name + "\" (" + string.Join(",", columns) + ") VALUES " + string.Join(",", valuesList);
                    using (NpgsqlCommand oCmd = new NpgsqlCommand(oString, myConnection))
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
    }
}
