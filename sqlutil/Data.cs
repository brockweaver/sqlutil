using Microsoft.VisualBasic.FileIO;
using sqlutil.Models;
using System.Text;

namespace sqlutil
{
    public static class Data
    {

        public static List<Table> ListTables(string dbConn)
        {
            return Sql.Read<Table>($@"
select 
	table_catalog as [catalog_name],
	table_schema as [schema_name],
	table_name as [table_name]
from information_schema.TABLES 
where table_type = 'BASE TABLE' 
order by 
    table_catalog, 
    table_schema, 
    table_name 
", dbConn);
        }

        public static List<Column> ListColumns(string dbConn, string schemaName, string tableName)
        {
            return Sql.Read<Column>($@"
select
	c.column_name as name,
	'[' + c.data_type + '] ' +
	case 
		when c.data_type in ('bit', 'uniqueidentifier', 'smallint', 'int', 'bigint', 'tinyint', 'datetime2', 'date', 'time', 'datetime', 'datetimeoffset') then ' '
		when c.data_type in ('nvarchar', 'varchar', 'char', 'nchar') then '(' + convert(varchar(50), c.character_maximum_length) + ') '
		when c.data_type in ('decimal', 'numeric') then '(' + convert(varchar(50), c.numeric_precision) + ', ' + convert(varchar(50), c.numeric_scale) + ') '
		else '' end +
	case when c.is_nullable = 'YES' then ' NULL ' else ' NOT NULL ' end +
	case when c.column_default is not null then ' DEFAULT ' + c.column_default + ' ' else '' end 
	as combined_data_type
from information_schema.columns c 
join information_schema.tables t 
    on c.table_name = t.table_name 
    and c.table_catalog = t.table_catalog 
    and c.table_schema = t.table_schema
where 
    t.Table_Type = 'BASE TABLE' 
    and c.table_schema = {schemaName} 
    and c.table_name = {tableName}
order by 
    c.Table_schema, 
    c.table_name, 
    c.ordinal_position
", dbConn);
        }

        public static void Wipe(string targetDbConn, TextWriter? output)
        {
            var tables = ListTablesInFKOrder(targetDbConn);
            tables.Reverse();

            output?.WriteLine($"Found {tables.Count} tables to delete");
            foreach (var t in tables)
            {
                output?.WriteLine($"Deleting [{t.SchemaName}].[{t.TableName}]...");
                Sql.WriteRaw($"delete from [{t.SchemaName}].[{t.TableName}]", targetDbConn);
            }
        }

        public static void Export(string sourceDbConn, string targetFilePath, TextWriter? output)
        {
            // init the file
            using var wtr = new StreamWriter(File.OpenWrite(targetFilePath));

            // get list of tables to export sorted by FK constraint hierarchy
            output?.WriteLine("Getting list of tables in foreign key order...");
            var tables = ListTablesInFKOrder(sourceDbConn);

            foreach (var t in tables)
            {
                output?.WriteLine($"[{t.SchemaName}].[{t.TableName}] : Begin export");

                // get list of columns for that table
                var columns = ListColumns(sourceDbConn, t.SchemaName, t.TableName);

                var countSql = $"select count(*) from [{t.SchemaName}].[{t.TableName}]";
                var count = (int)Sql.ReadValueRaw(countSql, sourceDbConn);

                output?.WriteLine($"[{t.SchemaName}].[{t.TableName}] : Found {count} rows to export");

                // generate insert into structure
                var sbInsert = new StringBuilder($"insert into [{t.SchemaName}].[{t.TableName}] (");
                sbInsert.Append(String.Join(", ", columns.Select(c => "[" + c.Name + "]").ToList()));
                sbInsert.Append(") values ");

                var insertStatement = sbInsert.ToString();

                wtr.WriteLine();
                wtr.WriteLine("------------------------------------------------------------------------------");
                wtr.WriteLine($"-- Begin table: [{t.SchemaName}].[{t.TableName}]");
                wtr.WriteLine("------------------------------------------------------------------------------");
                if (count == 0)
                {
                    wtr.WriteLine("-- No data in source table, skipping.");
                    wtr.WriteLine();
                    wtr.WriteLine("------------------------------------------------------------------------------");
                    wtr.WriteLine($"-- End table: [{t.SchemaName}].[{t.TableName}]");
                    wtr.WriteLine("------------------------------------------------------------------------------");
                    wtr.WriteLine("GO -- SQL_BATCH -- No rows in table");
                    wtr.WriteLine();

                    output?.WriteLine($"[{t.SchemaName}].[{t.TableName}] : No rows to export");

                }
                else
                {

                    // now let's stream data from the source table into a manageable chunk of values()
                    var select = $"select * from [{t.SchemaName}].[{t.TableName}]";
                    // we do "raw" here so table names are interpolated as normal strings and not parameterized.
                    // now append (r0c1, r0c2, r0c3, r0c4), (r1c1, r1c2, r1c3, r1c4) ...
                    var rowNumber = 0;
                    var totalRows = 0;
                    foreach (var item in Sql.StreamRaw(select, sourceDbConn))
                    {
                        if (rowNumber % 100 == 0)
                        {
                            // combine 100 rows into a single insert statement
                            wtr.WriteLine();
                            if (rowNumber > 0)
                            {
                                wtr.WriteLine($"GO -- SQL_BATCH -- {rowNumber} rows in batch");
                                output?.WriteLine($"[{t.SchemaName}].[{t.TableName}] : Exported {totalRows} rows");
                            }
                            rowNumber = 0;

                            wtr.WriteLine(insertStatement);
                        }
                        else
                        {
                            wtr.WriteLine(",");
                        }

                        var fileLine = CreateValuesList(item);
                        wtr.Write(fileLine);
                        rowNumber++;
                        totalRows++;

                    }
                    wtr.WriteLine();

                    wtr.WriteLine();
                    wtr.WriteLine("------------------------------------------------------------------------------");
                    wtr.WriteLine($"-- End table: [{t.SchemaName}].[{t.TableName}]");
                    wtr.WriteLine("------------------------------------------------------------------------------");
                    wtr.WriteLine($"GO -- SQL_BATCH -- {rowNumber} rows in batch, {totalRows} total rows");
                    wtr.WriteLine();

                    output?.WriteLine($"[{t.SchemaName}].[{t.TableName}] : Exported a total of {totalRows} rows");
                }

            }
            wtr.Close();
        }

        public static string CreateValuesList(List<object>? values)
        {
            if (values == null || values.Count == 0)
            {
                return "";
            }

            var rowString = new StringBuilder("(");
            for (var i = 0; i < values.Count; i++)
            {
                var val = values[i];

                if (val == null || val == DBNull.Value)
                {
                    rowString.Append("null");
                }
                else
                {
                    var valType = val.GetType();

                    if (valType == typeof(string)
                        || valType == typeof(Guid)
                        || valType == typeof(DateTime)
                        || valType == typeof(DateTimeOffset)
                        )
                    {
                        rowString.Append($"'{val.ToString().Replace("'", "''")}'");
                    }
                    else if (valType == typeof(bool))
                    {
                        rowString.Append($"{((bool)val ? "1" : "0")}");

                    }
                    else
                    {
                        rowString.Append($"{val}");
                    }
                }
                if (i < values.Count - 1)
                    rowString.Append(", ");
            }
            rowString.Append(") ");
            return rowString.ToString();
        }


        public static List<Table> ListTablesInFKOrder(string dbConn)
        {
            return Sql.Read<Table>($@"
-- this is the set of all tables in the database, one row per table. those with no FK to other tables will be set to sort_order = 1
drop table if exists #tables
create table #tables (table_schema varchar(50), table_name varchar(50), foreign_key_count int, sort_order int)

-- first pull all tables who have no foreign keys defined on them
insert into #tables (table_schema, table_name, foreign_key_count, sort_order) 
select  --*,
    table_schema, 
    table_name, 
    0, 
    -1
from information_schema.TABLES t
where t.table_schema not in ('sys')
	and t.table_type in ('BASE TABLE')
    and not exists (select top 1 1 from information_schema.TABLE_CONSTRAINTS tc where t.table_schema = tc.table_schema and t.table_name = tc.table_name and tc.constraint_type = 'FOREIGN KEY')
order by table_schema, table_name


--select * from information_schema.TABLE_CONSTRAINTS tc where tc.table_name = 'flight'

-- now pull all tables that DO have foreign keys and count how many they have (we'll need that later)
insert into #tables (table_schema, table_name, foreign_key_count, sort_order) 
select 
    tc.table_schema, 
    tc.table_name, 
    sum(case when tc.constraint_type = 'FOREIGN KEY' then 1 else 0 end) as Foreign_key_count, 
	0 as sort_order
from information_schema.TABLE_CONSTRAINTS tc
where 
    not exists (select top 1 1 from #tables t where tc.table_schema = t.table_schema and tc.table_name = t.table_name)
group by 
    tc.table_schema, 
    tc.table_name 
order by 
    sum(case when tc.constraint_type = 'FOREIGN KEY' then 1 else 0 end) , 
    tc.table_schema, 
    tc.table_name

-- base case: tables that do not point at any other tables
update #tables set sort_order = 1 where foreign_key_count = 0


drop table if exists #table_refs
create table #table_refs (table_schema varchar(50), table_name varchar(50), foreign_key_count int, sort_order int, fk_table_schema varchar(50), fk_table_name varchar(50), table_row_number int, total_mapped int)

insert into #table_refs (table_schema, table_name, foreign_key_count, sort_order, fk_table_schema, fk_table_name, table_row_number, total_mapped) 
select
t1.table_schema, 
	t1.table_name, 
	t1.foreign_key_count, 
	t1.sort_order, tcpk.table_schema as fk_table_schema, tcpk.table_name as fk_table_name,
row_number() over (partition by t1.table_schema, t1.table_name order by tcpk.table_name desc) as table_row_number,
0 as total_mapped
from #tables t1
join information_schema.TABLE_CONSTRAINTS tcfk
		on t1.table_schema = tcfk.table_schema
		and t1.table_name = tcfk.table_name
		and tcfk.constraint_type = 'FOREIGN KEY'
	join information_schema.REFERENTIAL_CONSTRAINTS rcpk
		on tcfk.constraint_schema = rcpk.constraint_schema
		and tcfk.constraint_name = rcpk.constraint_name
	join information_schema.TABLE_CONSTRAINTS tcpk
		on rcpk.unique_CONSTRAINT_schema = tcpk.CONSTRAINT_schema
		and rcpk.unique_CONSTRAINT_NAME = tcpk.CONSTRAINT_NAME
		and tcpk.constraint_type = 'PRIMARY KEY'
order by t1.table_schema, t1.table_name


--select * from #tables
--select * from #table_refs

--select * from #tables where sort_order > 0 order by sort_order, table_schema, table_name
--select * from #tables order by sort_order, table_schema, table_name
--select * from #table_refs order by sort_order, table_schema, table_name


declare @i int = 2
declare @rows int = 1
while @rows > 0 and @i < 100
begin

	update tr set
		total_mapped = (select count(*) from #tables t2 where t2.table_schema = tr.fk_table_schema and t2.table_name = tr.fk_table_name and t2.sort_order > 0)
	from #table_refs tr
	join #tables t1
		on t1.table_name = tr.table_name
		and t1.table_schema = tr.table_schema
		and tr.foreign_key_count != tr.total_mapped

	update t1 set
		sort_order = @i
	from #tables t1
	join (
		select 
			table_schema, table_name, sum(total_mapped) as grand_total_mapped
		from #table_refs
		group by table_schema, table_name
		having max(foreign_key_count) = sum(total_mapped) 
	) tr1
		on tr1.table_schema = t1.table_schema
		and tr1.table_name = t1.table_name
		and t1.sort_order = 0

	set @rows = @@rowcount

	set @i = @i + 1

end

select 
	--*
	table_schema as [schema_name],
	table_name as [table_name],
	sort_order as [cardinality]
from #tables order by sort_order, table_schema, table_name

--select * from #table_refs tr1
--join #table_refs tr2
--	on tr1.fk_table_schema = tr2.table_schema
--	and tr1.fk_table_name = tr2.table_name
--	and tr2.fk_table_schema = tr1.table_schema
--	and tr2.fk_table_name = tr1.table_name


", dbConn);

        }


        public static void Import(string sourceFilePath, string targetDbConn, TextWriter? output)
        {
            // open file
            var rdr = new StreamReader(File.OpenRead(sourceFilePath));

            // read each line for entire file
            var sb = new StringBuilder();
            var line = "";
            var batch = 1;
            var fullTableName = "";
            while (line != null)
            {
                line = rdr.ReadLine();
                if (line?.StartsWith("-- Begin table:") == true)
                {
                    fullTableName = line.Replace("-- Begin table:", "").Trim();
                    output?.WriteLine($"{fullTableName} : Begin data import");
                    batch = 0;
                }

                if (line?.StartsWith("GO -- SQL_BATCH --") == true)
                {
                    // we have a chunk of sql we need to run, let's do that now
                    var summary = line?.Replace("GO -- SQL_BATCH -- ", "").Replace("100 rows in batch", "");
                    output?.WriteLine($"{fullTableName} : Wrote {batch * 100} rows {summary}");

                    Sql.WriteRaw(sb.ToString(), targetDbConn);
                    // reset stringbuilder for a new chunk of sql
                    sb.Clear();
                    batch++;
                }
                else
                {
                    sb.AppendLine(line ?? "");
                }
            }

        }

        public static void Upload(string csvFilePath, string targetDbConn, TextWriter? output)
        {
            var parser = new TextFieldParser(csvFilePath);
            parser.TextFieldType = FieldType.Delimited;
            parser.SetDelimiters(new string[] { "," });

            var tableName = Path.GetFileNameWithoutExtension(csvFilePath);
            var columns = new List<Column>();

            var insertStatement = "";

            var rowCount = 0;

            while (!parser.EndOfData)
            {
                var row = parser.ReadFields();
                if (row != null)
                {
                    if (columns.Count == 0)
                    {
                        // we always assume first line is column names
                        foreach (var item in row)
                        {
                            columns.Add(new Column { Name = item.Replace("[", "").Replace("]", "") });
                        }
                    }
                    else
                    {
                        if (columns[0].CombinedDataType == "")
                        {
                            // first line of "real" data. this will determine the data type of the column in the database table.
                            for (var i = 0; i < columns.Count; i++)
                            {
                                var c = columns[i];
                                c.DeriveTypeInfo(row[i]);
                            }

                            // now we need to defin and create that table
                            var createSql = CreateTableStatement(tableName, columns);
                            Sql.WriteRaw($@"
drop table if exists [dbo].[{tableName}];
", targetDbConn);
                            Sql.WriteRaw(createSql, targetDbConn);
                            output?.WriteLine($"Created table [dbo].[{tableName}]");

                            var colNames = String.Join(", ", columns.Select(c => "[" + c.Name + "]"));
                            insertStatement = $"insert into [dbo].[{tableName}] ({colNames}) values (__VALS__)";
                        }

                        // create a valid insert statment and run it (once for each row, no transaction)
                        var insertSql = FormatInsert(insertStatement, columns, row);
                        Sql.WriteRaw(insertSql, targetDbConn);

                        if (rowCount % 100 == 0)
                        {
                            output?.WriteLine($"[dbo].[{tableName}] : Inserted {rowCount} rows");
                        }
                    }
                }

                rowCount++;
            }
        }

        private static string CreateTableStatement(string tableName, List<Column> columns)
        {
            var sql = $@"
create table [dbo].[{tableName}] (
    __COLS__
)
";
            var cols = new List<string>();
            foreach (var c in columns)
            {
                cols.Add($"[{c.Name}] {c.CombinedDataType}\n\t");
            }
            sql = sql.Replace("__COLS__", String.Join(", ", cols));

            return sql;

        }


        private static string FormatInsert(string insert, List<Column> columns, string[] row)
        {
            var values = new List<string>();
            for (var i = 0; i < columns.Count; i++)
            {
                var c = columns[i];
                var val = row[i];
                if (val == null || val == "")
                {
                    // null or empty is always the same as null
                    values.Add("null");
                }
                else if (c.Type == typeof(bool))
                {
                    values.Add(val?.ToLower() == "true" ||
                                val?.ToLower() == "y" ||
                                val?.ToLower() == "yes" ||
                                val?.ToLower() == "1" ?
                                "1" : "0");
                }
                else if (
                    c.Type == typeof(decimal) ||
                    c.Type == typeof(long))
                {
                    // bool or number, assume no string
                    values.Add(val);
                }
                else
                {
                    // guid, string, datetime, etc.
                    // assume we need to use quotes
                    values.Add("'" + val?.Replace("'", "''") + "'");
                }

            }

            var rv = insert.Replace("__VALS__", String.Join(", ", values));
            return rv;
        }

    }
}
