namespace sqlutil.Models
{
    public class Column
    {
        public string Name { get; set; } = "";
        public string CombinedDataType { get; set; } = "";
        public Type Type { get; set; }

        private static Type DeriveDataType(string input)
        {
            if (Guid.TryParse(input, out Guid g))
            {
                return typeof(Guid);
            }
            else if (bool.TryParse(input, out bool b))
            {
                return typeof(bool);
            }
            //else if (input.ToLower() == "y" || input.ToLower() == "true")
            //{
            //    return typeof(bool);
            //}
            else if (long.TryParse(input, out long l))
            {
                return typeof(long);
            }
            else if (DateOnly.TryParse(input, out DateOnly d))
            {
                return typeof(DateOnly);
            }
            else if (TimeOnly.TryParse(input, out TimeOnly t))
            {
                return typeof(TimeOnly);
            }
            else if (DateTime.TryParse(input, out DateTime dt))
            {
                return typeof(DateTime);
            }
            else if (decimal.TryParse(input, out Decimal dec))
            {
                return typeof(decimal);
            }
            else
            {
                return typeof(string);
            }
        }


        public void DeriveTypeInfo(string input)
        {
            Type = DeriveDataType(input);

            if (typeof(Guid) == Type)
            {
                CombinedDataType = "uniqueidentifier null";
            }
            else if (typeof(long) == Type)
            {
                CombinedDataType = "bigint null";
            }
            else if (typeof(bool) == Type)
            {
                CombinedDataType = "bit null";
            }
            else if (typeof(DateOnly) == Type)
            {
                CombinedDataType = "date null";
            }
            else if (typeof(TimeOnly) == Type)
            {
                CombinedDataType = "time null";
            }
            else if (typeof(DateTime) == Type)
            {
                CombinedDataType = "datetime2 null";
            }
            else if (typeof(decimal) == Type)
            {
                CombinedDataType = "decimal(25, 10) null";
            }
            else
            {
                CombinedDataType = "nvarchar(max) null";
            }
        }

        //public int? MaxLength { get; set; }
        //public int? Precision { get; set; }
        //public int? Scale { get; set; }
    }
}
