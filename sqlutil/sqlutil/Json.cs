using System.Text.Json;

namespace sqlutil
{
    internal class Json
    {
        public static string Stringify(object o)
        {
            return JsonSerializer.Serialize(o);
        }

        public static T? Parse<T>(string json)
        {
            return JsonSerializer.Deserialize<T>(json);
        }
    }
}
