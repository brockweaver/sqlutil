# sqlutil
Tools for import/export/clone of data to/from azure sql server

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

