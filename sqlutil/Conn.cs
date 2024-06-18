namespace sqlutil
{
    public static class Conn
    {
        private static Dictionary<string, string> __connections = new Dictionary<string, string>();

        private static string __filePath = "connections.json";

        /// <summary>
        /// Returns a list of key=value pairs for all items represented in this class
        /// </summary>
        /// <returns></returns>
        public static List<string> List()
        {
            var rv = new List<string>();
            foreach (var key in __connections.Keys)
            {
                rv.Add($"{key} = {__connections[key]}");
            }

            return rv;
        }

        /// <summary>
        /// Returns the value from connections.json file if given a keyname. if no entry is found, returns defaultValue ?? keyNameOrConnString. Never returns null.
        /// </summary>
        /// <param name="keyNameOrConnString"></param>
        /// <returns></returns>
        public static string Get(string keyNameOrConnString)
        {
            if (keyNameOrConnString.Contains(';') || keyNameOrConnString.Contains('='))
            {
                // assume a connection string.
                return keyNameOrConnString;
            }

            if (__connections.TryGetValue(keyNameOrConnString, out string? value))
            {
                return value!;
            }
            throw new InvalidOperationException($"Key named '{keyNameOrConnString}' was not found.");
        }

        /// <summary>
        /// Adds given keyName to connections.json and saves that file.
        /// </summary>
        /// <param name="keyName"></param>
        /// <param name="value"></param>
        public static void Add(string keyName, string value)
        {
            if (keyName.Contains(';') || keyName.Contains('='))
            {
                throw new InvalidOperationException($"Key name cannot contain ';' or '='");
            }
            __connections[keyName.ToLower()] = value;
            Conn.Save();
        }

        /// <summary>
        /// Removes given keyName from connections.json and saves that file.
        /// </summary>
        /// <param name="keyName"></param>
        public static bool Remove(string keyName)
        {
            if (!__connections.Remove(keyName.ToLower()))
                return false;

            Conn.Save();
            return true;
        }

        /// <summary>
        /// Writes a new connections.json file
        /// </summary>
        public static void Save()
        {
            File.WriteAllText(__filePath, Json.Stringify(__connections));
        }

        /// <summary>
        /// Reads connections.json file contents
        /// </summary>
        public static void Load()
        {
            var path = __filePath;
            var json = File.Exists(path) ? File.ReadAllText(path) : "{}";
            __connections = Json.Parse<Dictionary<string, string>>(json) ?? [];

        }
    }
}
