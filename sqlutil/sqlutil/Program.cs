namespace sqlutil;

public static class Program
{
    public static int Main(string[] args)
    {
        Conn.Load();

        switch (args.Length)
        {
            case 1:
                return OneArg(args[0]);
            case 2:
                return TwoArgs(args[0], args[1]);
            case 3:
                return ThreeArgs(args[0], args[1], args[2]);
            default:
                return Help();
        }
    }

    private static int OneArg(string arg0)
    {
        // sqlutil list
        if (arg0.ToLower() == "list")
        {
            Console.WriteLine(String.Join("\n", Conn.List()));
            return 0;
        }

        // none of the above.
        return Help();
    }

    private static int TwoArgs(string arg0, string arg1)
    {
        // sqlutil wipe [key_name | db_conn]
        if (arg0.ToLower() == "wipe")
        {
            var dbConn = Conn.Get(arg1);
            try
            {
                Console.WriteLine($"=> Wiping all data from {dbConn}...");
                Data.Wipe(dbConn);
                Console.WriteLine($"==> Wipe success!");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"==> Wipe failed: {ex.Message}");
                return 6;
            }
            return 0;
        }

        // sqlutil remove [key_name]
        if (arg0.ToLower() == "remove")
        {
            Console.WriteLine($"=> Removing {arg1}...");
            if (Conn.Remove(arg1))
                Console.WriteLine($"==> Removed key {arg1}");
            else
                Console.WriteLine($"==> Key {arg1} not found");
            return 0;
        }

        // sqlutil test [key_name | db_conn]
        if (arg0.ToLower() == "test")
        {
            var dbConn = Conn.Get(arg1);
            try
            {
                Console.WriteLine($"=> Testing connection to {dbConn}...");
                Sql.TestConnection(dbConn);
                Console.WriteLine($"==> Connection success!");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"==> Connection failed: {ex.Message}");
                return 1;
            }
            return 0;
        }

        // none of the above.
        return Help();
    }

    private static int ThreeArgs(string arg0, string arg1, string arg2)
    {

        // sqlutil add [key_name] [value]
        if (arg0.ToLower() == "add")
        {
            Console.WriteLine($"=> Adding key {arg1} = {arg2} ...");
            Conn.Add(arg1, arg2);
            Console.WriteLine($"==> Added key {arg0}");
            return 0;
        }

        // sqlutil export [src_key | src_conn_str] [output_file_path]
        if (arg0.ToLower() == "export")
        {
            var sourceDb = Conn.Get(arg1);
            var filePath = arg2;

            // if output file already exists, trash it first
            if (File.Exists(filePath))
                File.Delete(filePath);

            try
            {
                Console.WriteLine($"=> Testing connection to {sourceDb}...");
                Sql.TestConnection(sourceDb);
                Console.WriteLine($"==> Connection success!");

                Console.WriteLine($"=> Begin exporting data from {sourceDb} to {filePath} ...");
                Data.Export(sourceDb, filePath);
                Console.WriteLine($"==> Done exporting data from {sourceDb} to {filePath}.");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                return 2;
            }

            return 0;

        }

        // sqlutil import [input_file_path] [tgt_key | tgt_conn_str]
        if (arg0.ToLower() == "import")
        {
            var filePath = arg1;
            var targetDb = Conn.Get(arg2);


            try
            {
                // if input file doesn't exist, bomb
                if (!File.Exists(filePath))
                    throw new InvalidOperationException($"No file found at {filePath}");

                Console.WriteLine($"=> Testing connection to {targetDb}...");
                Sql.TestConnection(targetDb);
                Console.WriteLine($"==> Connection success!");

                Console.WriteLine($"=> Begin importing data from {filePath} to {targetDb} ...");
                Data.Import(filePath, targetDb);
                Console.WriteLine($"==> Done importing data from {filePath} to {targetDb}.");

            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                return 3;
            }


            return 0;

        }

        // sqlutil copy [src_key | src_conn_str] [tgt_key | tgt_conn_str]
        if (arg0.ToLower() == "copy")
        {
            var sourceDb = Conn.Get(arg1);
            var targetDb = Conn.Get(arg2);
            var filePath = Path.GetTempFileName();

            try
            {
                Console.WriteLine($"=> Testing connection to {sourceDb}...");
                Sql.TestConnection(sourceDb);
                Console.WriteLine($"==> Connection success!");

                Console.WriteLine($"=> Testing connection to {targetDb}...");
                Sql.TestConnection(targetDb);
                Console.WriteLine($"==> Connection success!");

                Console.WriteLine($"=> Begin exporting data from {sourceDb} to {filePath} ...");
                Data.Export(sourceDb, filePath);
                Console.WriteLine($"==> Done exporting data from {sourceDb} to {filePath}.");

                Console.WriteLine($"==> Begin importing data from {filePath} to {targetDb} ...");
                Data.Import(filePath, targetDb);
                Console.WriteLine($"==> Done importing data from {filePath} to {targetDb}.");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(ex.Message);
                return 4;
            }

            return 0;

        }


        // none of the above.
        return Help();
    }

    public static int Help()
    {
        Console.WriteLine(@"
sqlutil <cmd> [options]
    export [source_db] [output_file_path]
        source_db = [connections.json keyname for source_db | full connection string for source_db]
        copies all data from source_db and writes into file at output_file_path

    import [input_file_path] [target_db]
        target_db = [connections.json keyname for target_db | full connection string for target_db]
        copies data from file at input_file_path and inserts it into the database at target_db
        will do so in a foreign-key aware order, so FK constraints do not need to be disabled or ignored

    copy [source_db] [target_db]
        source_db = [connections.json keyname for source_db | full connection string for source_db]
        target_db = [connections.json keyname for target_db | full connection string for target_db]
        copies data from source_db to target_db
        this is the same as running the following commands:
            sqlutil export [source_db] dumpfile.txt data
            sqlutil import dumpfile.txt [target_db]
    wipe [target_db]
        target_db = [connections.json keyname for target_db | full connection string for target_db]
        deletes all data in the database in proper FK order and leaves schema intact

    add [key_name] [full_connection_string]
        adds new entry to connections.json with given key name and value.
        if entry exists, value is overwritten

    remove [key_name]
        removes given entry from connections.json. does not touch corresponding database.

    test [key_name | full connection string to db]
        tests connection to db, either by looking up its key_name or using the given string as a connection string

    list
        lists all entries in the connections.json file
    
");

        return 0;
    }
}