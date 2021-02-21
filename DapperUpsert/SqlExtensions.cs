using System;
using System.Collections.Generic;
using System.Text;
using Dapper;
using System.Threading.Tasks;
using System.Data;
using System.Reflection;
using System.Linq;
using Dapper.Contrib.Extensions;
using System.Collections;

namespace Dapper.Contrib.Extensions.Upsert
{
    public static partial class SqlExtensions
    {
        /// <summary>
        /// Generic Upsert records in database. If record exists based on Primary key matching, ALL of the records columns/fields will be updated.
        /// If no key exists, the record will be added.
        /// </summary>
        /// <typeparam name="T">Dapper.Contrib POCO class with correct attributes(Key, etc) Maybe be a single or generic IEnumerable </typeparam>
        /// <param name="db">The database</param>
        /// <param name="entitiesToUpsert">Single or generic IEnumerable entites to Upsert</param>
        /// <param name="transaction">Specify a transaction if required</param>
        /// <param name="commandTimeout">Seconds</param>
        /// <returns>Number of records affected</returns>
        public static async Task<int> UpsertAsync<T>(this IDbConnection db, 
                                                     T entitiesToUpsert, 
                                                     IDbTransaction transaction = null,
                                                     int? commandTimeout = null) where T : class
        {
            var contribType = typeof(SqlMapperExtensions);

            var type = typeof(T);
            
            var isList = false;
            if (type.IsArray)
            {
                type = type.GetElementType();
                isList = true;
            }
            else if (type.IsGenericType)
            {
                var typeInfo = type.GetTypeInfo();
                bool implementsGenericIEnumerableOrIsGenericIEnumerable =
                    typeInfo.ImplementedInterfaces.Any(ti => ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
                    typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

                if (implementsGenericIEnumerableOrIsGenericIEnumerable)
                {
                    type = type.GetGenericArguments()[0];
                    isList = true;
                }

            }

            IEnumerable entities = null;
            if (!isList)
            {
                var listType = typeof(List<>);
                var constructedListType = listType.MakeGenericType(type);
                var instance = Activator.CreateInstance(constructedListType) as IList;
                instance.Add(entitiesToUpsert);
                entities = instance;
            }
            else
            {
                entities = entitiesToUpsert as IEnumerable;
            }
            

            var tableName = contribType.GetTableName(type); //GetTableName
            var sbColumnList = new StringBuilder(null);
            var allProperties = contribType.TypePropertiesCache(type); //TypePropertiesCache(type);
            var keyProperties = contribType.KeyPropertiesCache(type);// KeyPropertiesCache(type).ToList();
            var computedProperties = contribType.ComputedPropertiesCache(type);// ComputedPropertiesCache(type);
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

            //added need to include key column for upsert
            var allPropertiesExceptComputed = allProperties.Except(computedProperties).ToList();

            var explicitKeyProperties = contribType.ExplicitKeyPropertiesCache(type); // ExplicitKeyPropertiesCache(type);
            if (keyProperties.Count == 0 && explicitKeyProperties.Count == 0)
                throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            keyProperties.AddRange(explicitKeyProperties);

            var columns = allPropertiesExceptComputed.Select(x => x.Name).ToList();

            var dbConnectionType = db.GetType().Name;
            int result;
            switch (dbConnectionType)
            {
                case "SQLiteConnection":
                    result = await db.ReplaceInto<T>(tableName, columns, entities, transaction, commandTimeout);
                    break;
                case "MySqlConnection":
                    result = await db.MySQLUpsert<T>(tableName, columns, entities, transaction, commandTimeout);
                    break;
                default:
                    throw new Exception($"No method found for database type: {dbConnectionType}");
            }
            return result;
        }

        /// <summary>
        /// Upsert records in database. If record exists based on Primary key matching, ALL of the records columns/fields will be updated.
        /// If no key exists, the record will be added.
        /// </summary>
        /// <param name="db">The database to upsert</param>
        /// <param name="tableName">Name of the Table</param>
        /// <param name="columns">Columns in the database</param>
        /// <param name="records">IEnumerable of the records to Upsert</param>
        /// <param name="transaction">Specify a transaction is required</param>
        /// <param name="commandTimeout">Seconds</param>
        /// <returns></returns>
        public static async Task<int> UpsertAsync(this IDbConnection db,
                                                  string tableName,
                                                  List<string> columns, 
                                                  IEnumerable<dynamic> records, 
                                                  IDbTransaction transaction = null,
                                                  int? commandTimeout = null)
        {
            var dbConnectionType = db.GetType().Name;
            int result;
            switch (dbConnectionType)
            {
                case "SQLiteConnection":
                    result = await db.ReplaceInto(tableName, columns, records, transaction, commandTimeout);
                    break;
                case "MySqlConnection":
                    result = await db.MySQLUpsert(records, columns, tableName, transaction, commandTimeout);
                    break;
                default:
                    throw new Exception($"No method found for database type: {dbConnectionType}");
            }
            return result;
        }

        private static async Task<int> MySQLUpsert(this IDbConnection db, 
                                                   string tableName, 
                                                   List<string> columns, 
                                                   List<string> parameterizedValues, 
                                                   DynamicParameters parameters, 
                                                   IDbTransaction transaction=null,
                                                   int? commandTimeout = null)
        {
            var newList = new List<string>();

            foreach (var c in columns)
            {
                newList.Add($"{c} = VALUES({c})");
            }

            var cmd = $"INSERT INTO {tableName} ({String.Join(",", columns)}) VALUES {String.Join(",", parameterizedValues)} ON DUPLICATE KEY UPDATE {String.Join(",", newList)}";
            return await db.ExecuteAsync(cmd, parameters,transaction, commandTimeout);
        }

        private static async Task<int> MySQLUpsert(this IDbConnection db, 
                                                   IEnumerable<dynamic> records, 
                                                   List<string> columns, 
                                                   string tableName, 
                                                   IDbTransaction transaction = null,
                                                   int? commandTimeout = null)
        {
            long i = 0;
            var dynamicParams = new DynamicParameters();
            var items = new List<string>();
            foreach (var r in records)
            {
                var valueSb = new StringBuilder();
                valueSb.Append("(");
                var record = (IDictionary<string, object>)r;
                var pList = new List<string>();
                foreach (var v in record.Values)
                {
                    string p1 = $"P{i}";
                    dynamicParams.Add(p1, v);
                    pList.Add($"@{p1}");
                    i++;
                }
                valueSb.Append(String.Join(",", pList));
                valueSb.Append(")");
                items.Add(valueSb.ToString());
            }

            return await db.MySQLUpsert(tableName, columns, items, dynamicParams, transaction, commandTimeout);
        }

        private static Type GetTypeOrGenericType(Type Tentity)
        {
            //var type = typeof(Tentity);
            var type = Tentity;
            if (type.IsGenericType)
            {
                var typeInfo = type.GetTypeInfo();
                bool implementsGenericIEnumerableOrIsGenericIEnumerable =
                    typeInfo.ImplementedInterfaces.Any(ti => ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
                    typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

                if (implementsGenericIEnumerableOrIsGenericIEnumerable)
                {
                    type = type.GetGenericArguments()[0];
                }

            }
            return type;
        }

        private static async Task<int> MySQLUpsert<Tentity>(this IDbConnection db,
                                                            string tableName,
                                                            List<string> columns,
                                                            IEnumerable records, 
                                                            IDbTransaction transaction = null,
                                                            int? commandTimeout = null)
        {
            var sb = new StringBuilder();

            long i = 0;
            var dynamicParams = new DynamicParameters();
            var items = new List<string>();
            var type = GetTypeOrGenericType(typeof(Tentity));

            var valueSb = new StringBuilder();
            foreach (var r in records)
            {
                valueSb.Append("(");
                var pList = new List<string>();
                foreach (var column in columns)
                {
                    var value = type.GetProperty(column)?.GetValue(r, null);
                    string p1 = $"P{i}";
                    dynamicParams.Add(p1, value);
                    pList.Add($"@{p1}");
                    i++;
                }
                valueSb.Append(String.Join(",", pList));
                valueSb.Append(")");
                items.Add(valueSb.ToString());
                valueSb.Clear();
            }
            return await db.MySQLUpsert(tableName, columns, items, dynamicParams, transaction, commandTimeout);
        }

        private static async Task<int> ReplaceInto(this IDbConnection db,
                                       string intoTableName,
                                       List<string> columns,
                                       IEnumerable<dynamic> entitiesToReplaceInto, 
                                       IDbTransaction transaction = null,
                                       int? commandTimeout = null)
        {
            var valueSb = new StringBuilder();
            var inserts = new List<string>();
            var dynamicParams = new DynamicParameters();
            long i = 0;
            foreach (IDictionary<string, object>record in entitiesToReplaceInto)
            {
                var valueList = new List<string>();
                foreach (var column in columns)
                {
                    var p = $"p{i}";
                    dynamicParams.Add(p, record[column]);
                    valueList.Add($"@{p}");
                    i++;
                }
                valueSb.Append("(");
                valueSb.Append(String.Join(",", valueList));
                valueSb.Append(")");
                inserts.Add(valueSb.ToString());
                valueSb.Clear();
            }
            return await db.ReplaceInto(intoTableName, columns, inserts, dynamicParams, transaction, commandTimeout);
        }

        private static async Task<int> ReplaceInto(this IDbConnection db, 
                                                   string tableName, 
                                                   List<string> columns, 
                                                   List<string> recordInserts, 
                                                   DynamicParameters parameters, 
                                                   IDbTransaction transaction,
                                                   int? commandTimeout=null)
        {
            var cmd = $"REPLACE INTO {tableName} ({String.Join(",", columns)}) VALUES {String.Join(",", recordInserts)}";
            return await db.ExecuteAsync(cmd, parameters, transaction, commandTimeout);
        }

        private static async Task<int> ReplaceInto<Tentity>(this IDbConnection db,
                                                            string tableName,
                                                            List<string> columns,
                                                            IEnumerable records, 
                                                            IDbTransaction transaction = null,
                                                            int? commandTimeout = null)
        {
            var valueSb = new StringBuilder();
            var inserts = new List<string>();
            var dynamicParams = new DynamicParameters();
            long i = 0;

            var type = GetTypeOrGenericType(typeof(Tentity));

            foreach (var r in records)
            {
                var valueList = new List<string>();
                foreach (var column in columns)
                {
                    var value = type.GetProperty(column)?.GetValue(r, null);
                    var p = $"p{i}";
                    dynamicParams.Add(p, value);
                    valueList.Add($"@{p}");
                    i++;
                }
                valueSb.Append("(");
                valueSb.Append(String.Join(",", valueList));
                valueSb.Append(")");
                inserts.Add(valueSb.ToString());
                valueSb.Clear();
            }
            return await db.ReplaceInto(tableName, columns, inserts, dynamicParams, transaction, commandTimeout);
        }

        //Todo: Fix and test this
        private static async Task BulkInsert(IDbConnection db, dynamic records, string intoTableName)
        {
            var SqlSb = new StringBuilder($"INSERT INTO {intoTableName} (");
            var first = (IDictionary<string, object>)records[0];
            long i = 0;
            foreach (var c in first.Keys) // (int i = 0; i< first.Keys.Count; i++)
            {
                SqlSb.Append(c);
                i++;
                SqlSb.Append((i == first.Keys.Count) ? ") VALUES " : ", ");
            }
            var valueSb = new StringBuilder();
            var inserts = new List<string>();
            foreach (var r in records)
            {
                valueSb.Append("(");
                var record = (IDictionary<string, object>)r;
                i = 0;
                foreach (var v in record.Values)// (int i = 0; i < first.Keys.Count; i++)
                {
                    valueSb.Append((!String.IsNullOrEmpty(v?.ToString()) ? $"'{v?.ToString()}'" : "NULL"));
                    i++;
                    valueSb.Append((i == first.Keys.Count) ? ") " : ", ");
                }
                inserts.Add(valueSb.ToString());
                valueSb.Clear();
                //break;
            }
            SqlSb.Append(String.Join(", ", inserts));
            //Console.WriteLine(SqlSb.ToString());
            var result = await db.ExecuteAsync(SqlSb.ToString());
            //return; Task.FromResult(0);
        }
        //Todo: Fix this bulk update
        private static async Task BulkUpdate(IDbConnection db, List<string> columns, string tableToUpdate, string tempTableName)
        {
            var sb = new StringBuilder();
            sb.Append($"UPDATE {tableToUpdate} as O SET ");
            var columnEqualList = new List<string>();
            foreach (var c in columns)
            {
                columnEqualList.Add($"O.{c} = N.{c}");
            }
            sb.Append(string.Join(",", columnEqualList));
            sb.Append($" FROM O INNER JOIN {tempTableName} as N ON O.Id=N.Id");
            var result = await db.ExecuteAsync(sb.ToString());
        }

        private static async Task<string> CreateTempTable(IDbConnection db, string localTableName)
        {
            var tempTableName = localTableName + Guid.NewGuid().ToString("N");
            var create = $"CREATE TABLE {tempTableName} AS SELECT * FROM {localTableName} WHERE 0";
            await db.ExecuteAsync(create);
            return tempTableName;// Task.FromResult(tempTableName);
        }

        private static async Task DropTable(IDbConnection db, string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                return;
            }
            await db.ExecuteAsync($"DROP TABLE {tableName}");
            //return Task.FromResult(0);
        }

    }
}
