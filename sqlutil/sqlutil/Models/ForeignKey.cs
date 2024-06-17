namespace sqlutil.Models
{
    internal class ForeignKey
    {
        public string? Name { get; set; }
        public Table? SourceTable { get; set; }
        public Column? SourceColumn { get; set; }
        public Table? TargetTable { get; set; }
        public Column? TargetColumn { get; set; }
    }
}
