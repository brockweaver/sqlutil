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

        public static void Wipe(string targetDbConn)
        {
            var tables = ListTablesInFKOrder(targetDbConn);
            tables.Reverse();

            foreach (var t in tables)
            {
                Sql.WriteRaw($"delete from [{t.SchemaName}].[{t.TableName}]", targetDbConn);
            }
        }

        public static void Export(string sourceDbConn, string targetFilePath)
        {
            // init the file
            using var wtr = new StreamWriter(File.OpenWrite(targetFilePath));

            // get list of tables to export sorted by FK constraint hierarchy
            var tables = ListTablesInFKOrder(sourceDbConn);

            foreach (var t in tables)
            {

                // get list of columns for that table
                var columns = ListColumns(sourceDbConn, t.SchemaName, t.TableName);

                var countSql = $"select count(*) from [{t.SchemaName}].[{t.TableName}]";
                var count = (int)Sql.ReadValueRaw(countSql, sourceDbConn);

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
                                wtr.WriteLine($"GO -- SQL_BATCH -- {rowNumber} rows in batch");
                            rowNumber = 0;

                            wtr.WriteLine(insertStatement);
                        }
                        else
                        {
                            wtr.WriteLine(",");
                        }

                        var outputLine = CreateValuesList(item);
                        wtr.Write(outputLine);
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

insert into #tables (table_schema, table_name, foreign_key_count, sort_order) 
select table_schema, table_name, sum(case when constraint_type = 'FOREIGN KEY' then 1 else 0 end) as Foreign_key_count, 0 as sort_order
from information_schema.TABLE_CONSTRAINTS group by table_schema, table_name order by sum(case when constraint_type = 'FOREIGN KEY' then 1 else 0 end) , table_schema, table_name

if (select count(*) from #tables) = 0
begin
    -- no tables have any FK values. weird but ok.
    -- just pull in all tables defined in the schema.

    insert into #tables (table_schema, table_name, foreign_key_count, sort_order) 
    select table_schema, table_name, 0, 0
    from information_schema.TABLES t
    order by table_schema, table_name

end


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


--select * from #tables where sort_order > 0 order by sort_order, table_schema, table_name
--select * from #tables order by sort_order, table_schema, table_name
--select * from #table_refs order by sort_order, table_schema, table_name


declare @i int = 2
declare @rows int = 1
while @rows > 0 and @i < 50
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
	table_schema as [schema_name],
	table_name as [table_name]
from #tables order by sort_order, table_schema, table_name


", dbConn);

        }


        public static void Import(string sourceFilePath, string targetDbConn)
        {
            // open file
            var rdr = new StreamReader(File.OpenRead(sourceFilePath));

            // read each line for entire file
            var sb = new StringBuilder();
            var line = "";
            var batch = 0;
            while (line != null)
            {
                line = rdr.ReadLine();
                if (line?.StartsWith("-- Begin table:") == true)
                {
                    Console.WriteLine(line);
                    batch = 0;
                }

                if (line == null || line.StartsWith("GO -- SQL_BATCH --"))
                {
                    // we have a chunk of sql we need to run, let's do that now
                    Console.WriteLine($"Writing batch {batch} ({line?.Replace("GO -- SQL_BATCH -- ", "")})...");
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


    }
}
