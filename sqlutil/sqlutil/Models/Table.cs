namespace sqlutil.Models
{
    internal class Table
    {
        public string CatalogName { get; set; } = "";
        public string SchemaName { get; set; } = "";
        public string TableName { get; set; } = "";
        public List<Column> PrimaryKeys { get; set; } = [];
        public List<ForeignKey> ForeignKeys { get; set; } = [];
    }
}
