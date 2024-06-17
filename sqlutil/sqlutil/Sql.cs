using Microsoft.Data.SqlClient;
using System.Reflection;

namespace sqlutil
{
    internal static class Sql
    {
        public static int WriteRaw(string sql, string dbConn)
        {
            try
            {
                using var conn = new SqlConnection(dbConn);
                conn.Open();
                using var cmd = CreateCommand(sql, null, conn);
                return cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message + "\nSource SQL: " + sql, ex);
            }

        }

        public static int Write(FormattableString sql, string dbConn)
        {
            try
            {
                using var conn = new SqlConnection(dbConn);
                conn.Open();
                using var cmd = CreateCommand(sql, conn);
                return cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message + "\nSource SQL: " + sql.Format, ex);
            }

        }

        public static object ReadValueRaw(string sql, string dbConn)
        {
            try
            {
                using var conn = new SqlConnection(dbConn);
                conn.Open();
                using var cmd = CreateCommand(sql, null, conn);
                return cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message + "\nSource SQL: " + sql, ex);
            }

        }

        public static object ReadValue(FormattableString sql, string dbConn)
        {
            try
            {
                using var conn = new SqlConnection(dbConn);
                conn.Open();
                using var cmd = CreateCommand(sql, conn);
                return cmd.ExecuteScalar();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message + "\nSource SQL: " + sql.Format, ex);
            }

        }

        public static List<T> Read<T>(FormattableString sql, string dbConn) where T : class, new()
        {
            // get all public properties on the given type
            var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
            var map = new List<PropertyInfo?>();

            try
            {
                using var conn = new SqlConnection(dbConn);
                conn.Open();
                using var cmd = CreateCommand(sql, conn);
                using var rdr = cmd.ExecuteReader();

                // map reader fields to properties on the given type
                for (var i = 0; i < rdr.FieldCount; i++)
                {
                    var fieldName = rdr.GetName(i);
                    var prop = props.FirstOrDefault(x => x.Name.ToLower().Replace("_", "") == fieldName.ToLower().Replace("_", ""));
                    map.Add(prop);
                }

                var rv = new List<T>();

                while (rdr.Read())
                {
                    var item = new T();
                    // read properties into the type
                    for (var i = 0; i < rdr.FieldCount; i++)
                    {
                        var prop = map[i];
                        if (prop != null && !rdr.IsDBNull(i))
                        {
                            prop.SetValue(item, rdr.GetValue(i));
                        }
                    }
                    rv.Add(item);

                }
                return rv;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message + $"\nSource SQL: {sql.ToString()}\nSQL Params: {String.Join(", ", sql.GetArguments())}", ex);
            }

        }
        public static IEnumerable<List<object>> StreamRaw(string sql, string dbConn)
        {
            // can't have a yield return wrapped in a try/catch so that's why this looks so weird
            SqlConnection? conn = null;
            SqlCommand? cmd = null;
            SqlDataReader? rdr = null;
            try
            {
                conn = new SqlConnection(dbConn);
                conn.Open();
                cmd = CreateCommand(sql, null, conn);
                rdr = cmd.ExecuteReader();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message + "\nSource SQL: " + sql, ex);
            }

            while (rdr.Read())
            {
                var item = new List<object>();
                // read properties into the type
                for (var i = 0; i < rdr.FieldCount; i++)
                {
                    item.Add(rdr.GetValue(i));
                }
                yield return item;

            }

            cmd.Dispose();
            conn.Close();
            conn.Dispose();


        }

        public static IEnumerable<List<object>> Stream(FormattableString sql, string dbConn)
        {

            // can't have a yield return wrapped in a try/catch so that's why this looks so weird
            SqlConnection? conn = null;
            SqlCommand? cmd = null;
            SqlDataReader? rdr = null;
            try
            {
                conn = new SqlConnection(dbConn);
                conn.Open();
                cmd = CreateCommand(sql, conn);
                for (var i = 0; i < sql.ArgumentCount; i++)
                {
                    var arg = sql.GetArgument(i);
                    cmd.Parameters.Add(new SqlParameter(i.ToString(), arg));
                }
                rdr = cmd.ExecuteReader();
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(ex.Message + "\nSource SQL: " + sql.Format, ex);
            }

            while (rdr.Read())
            {
                var item = new List<object>();
                // read properties into the type
                for (var i = 0; i < rdr.FieldCount; i++)
                {
                    item.Add(rdr.GetValue(i));
                }
                yield return item;

            }

            cmd.Dispose();
            conn.Close();
            conn.Dispose();

        }

        /// <summary>
        /// Attempts to connect to given database. Throws error if it fails, otherwise just returns.
        /// </summary>
        /// <param name="dbConn"></param>
        public static void TestConnection(string dbConn)
        {
            var builder = new SqlConnectionStringBuilder(dbConn);
            builder.ConnectTimeout = 30; // Azure sql dbs require 30 second minimum timeout according to docs

            var conn = new SqlConnection(builder.ToString());
            try
            {
                // just attempt to connect then close it
                conn.Open();
                conn.Close();
            }
            //catch (Exception ex)
            //{
            //    //Debug.WriteLine(ex.Message);
            //    throw;
            //}
            finally
            {
                conn.Dispose();
            }
        }

        public static Tuple<string, IEnumerable<SqlParameter>> InterpolateSql(FormattableString sql)
        {
            var formatProvider = new SqlFormatProvider();
            var stmt = sql.ToString(formatProvider);
            var prms = new List<SqlParameter>();
            for (var i = 0; i < sql.ArgumentCount; i++)
            {
                var arg = sql.GetArgument(i);
                prms.Add(new SqlParameter("@p" + i.ToString(), arg));
            }
            return new Tuple<string, IEnumerable<SqlParameter>>(stmt, prms);
        }

        public static SqlCommand CreateCommand(FormattableString sql, SqlConnection conn)
        {
            var tup = InterpolateSql(sql);
            return CreateCommand(tup.Item1, tup.Item2, conn);

        }

        public static SqlCommand CreateCommand(string sql, IEnumerable<SqlParameter>? prms, SqlConnection conn)
        {
            var rv = new SqlCommand(sql, conn);
            if (prms != null)
            {
                foreach (var p in prms)
                {
                    rv.Parameters.Add(p);
                }
            }

            return rv;
        }


    }
}
