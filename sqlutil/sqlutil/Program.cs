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
        if (arg0.ToLower() == "list_keys")
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
        if (arg0.ToLower() == "wipe_data")
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
        if (arg0.ToLower() == "remove_key")
        {
            Console.WriteLine($"=> Removing {arg1}...");
            if (Conn.Remove(arg1))
                Console.WriteLine($"==> Removed key {arg1}");
            else
                Console.WriteLine($"==> Key {arg1} not found");
            return 0;
        }

        // sqlutil test [key_name | db_conn]
        if (arg0.ToLower() == "test_connection")
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
        if (arg0.ToLower() == "add_key")
        {
            Console.WriteLine($"=> Adding key {arg1} = {arg2} ...");
            Conn.Add(arg1, arg2);
            Console.WriteLine($"==> Added key {arg0}");
            return 0;
        }

        // sqlutil export [src_key | src_conn_str] [output_file_path]
        if (arg0.ToLower() == "export_data")
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
        if (arg0.ToLower() == "import_data")
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
        if (arg0.ToLower() == "copy_data")
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

                Console.WriteLine($"=> Begin wiping data from {targetDb} ...");
                Data.Wipe(targetDb);
                Console.WriteLine($"==> Done wiping data from {targetDb}.");

                Console.WriteLine($"=> Begin importing data from {filePath} to {targetDb} ...");
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

    test_connection [key_name | full_connection_string]
        tests connection to db, either by looking up its key_name or using the given string as a connection string

    export_data [source_db] [output_file_path]
        copies all data from source_db and writes into file at output_file_path
        source_db = [key_name | full_connection_string]

    import_data [input_file_path] [target_db]
        copies data from file at input_file_path and inserts it into the database at target_db
        will do so in a foreign-key aware order, so FK constraints do not need to be disabled or ignored
        target_db = [key_name | full_connection_string]

    wipe_data [target_db]
        deletes all data in the database in proper FK order and leaves schema intact
        target_db = [key_name | full_connection_string]

    copy_data [source_db] [target_db]
        copies data from source_db to target_db
        this is the same as running the following commands:
            sqlutil export_data [source_db] dumpfile.txt data
            sqlutil wipe_data [target_db]
            sqlutil import_data dumpfile.txt [target_db]
        source_db = [key_name | full_connection_string]
        target_db = [key_name | full_connection_string]

    list_keys
        lists all entries in the connections.json file
    
    add_key [key_name] [full_connection_string]
        adds new entry to connections.json with given key name and value.
        if entry exists, value is overwritten

    remove_key [key_name]
        removes given entry from connections.json. does not touch corresponding database.

");

        return 0;
    }
}