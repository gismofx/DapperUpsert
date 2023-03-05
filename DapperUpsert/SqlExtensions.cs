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
using System.Drawing;
using System.Data.Common;
using System.Runtime.CompilerServices;

namespace Dapper.Contrib.Extensions.Upsert
{
    public static partial class SqlExtensions
    {
        //Todo: Add MSsql and PostgreSQL support

        /// <summary>
        /// Insert Multiple Records in one execution.
        /// Chunk size will limit how many records are included in a single statement
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="entitiesToInsert"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        public static async Task<int> BulkInsertAsync<T>(this IDbConnection db,
                                                       IEnumerable<T> entitiesToInsert,
                                                       int chunkSize = 1000,
                                                       IDbTransaction transaction = null,
                                                       int? commandTimeout = null)
        {
            var contribType = typeof(SqlMapperExtensions);
            var tableName = contribType.GetTableName(typeof(T));
            var columnsProperties = GetAllColumns<T>();
            var columns = columnsProperties.Select(x => x.Name);

            //Setup the statement
            var SqlSb = new StringBuilder($"INSERT INTO {tableName} ");
            SqlSb.AppendLine($"({string.Join(",", columns)}) VALUES");

            BuildInsertParameters(columns, entitiesToInsert);

            var entityType = typeof(T);
            //var valueSb = new StringBuilder();
            int result = 0;
            foreach (var entityChunk in entitiesToInsert.Chunk2(1000))
            {
                var bulkInsertSb = new StringBuilder(SqlSb.ToString());
                var insertParams = BuildInsertParameters(columns, entityChunk);
                bulkInsertSb.AppendLine(string.Join(",", insertParams.ParameterizedInsertValues));
                result += await db.ExecuteAsync(bulkInsertSb.ToString(), insertParams.DynamicParams, transaction, commandTimeout);
            }
            return result;
        }


        public static async Task<int> UpsertAsync<T>(this IDbConnection db,
                                                     IEnumerable<T> entitiesToUpsert,
                                                     int chunkSize = 1000,
                                                     IDbTransaction transaction = null,
                                                     int? commandTimeout = null)
        {
            var type = typeof(T);
            var contribType = typeof(SqlMapperExtensions);
            var tableName = contribType.GetTableName(type);
            var columnsProperties = GetAllColumns<T>();
            var columns = columnsProperties.Select(x => x.Name);

            var explicitKeyProperties = contribType.ExplicitKeyPropertiesCache(type);
            var keyProperties = contribType.KeyPropertiesCache(type);
            if (keyProperties.Count == 0 && explicitKeyProperties.Count == 0)
                throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

            var dbConnectionType = db.GetType().Name;
            int result;
            switch (dbConnectionType)
            {
                case "SQLiteConnection":
                    result = await db.ReplaceInto<T>(tableName, columns, entitiesToUpsert, transaction, commandTimeout);
                    break;
                case "MySqlConnection":
                    result = await db.MySQLUpsertAsync<T>(entitiesToUpsert, columns, tableName, chunkSize, transaction, commandTimeout);
                    break;
                default:
                    throw new Exception($"No method found for database type: {dbConnectionType}");
            }
            return result;


        }

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
        [Obsolete("Use Generic Method", true)]
        //public static async Task<int> UpsertAsync<T>(this IDbConnection db,
        //                                             T entitiesToUpsert,
        //                                             IDbTransaction transaction = null,
        //                                             int? commandTimeout = null) where T : class
        //{
        //    var contribType = typeof(SqlMapperExtensions);

        //    var type = typeof(T);

        //    var isList = false;
        //    if (type.IsArray)
        //    {
        //        type = type.GetElementType();
        //        isList = true;
        //    }
        //    else if (type.IsGenericType)
        //    {
        //        var typeInfo = type.GetTypeInfo();
        //        bool implementsGenericIEnumerableOrIsGenericIEnumerable =
        //            typeInfo.ImplementedInterfaces.Any(ti => ti.IsGenericType && ti.GetGenericTypeDefinition() == typeof(IEnumerable<>)) ||
        //            typeInfo.GetGenericTypeDefinition() == typeof(IEnumerable<>);

        //        if (implementsGenericIEnumerableOrIsGenericIEnumerable)
        //        {
        //            type = type.GetGenericArguments()[0];
        //            isList = true;
        //        }

        //    }

        //    IEnumerable entities = null;
        //    if (!isList)
        //    {
        //        var listType = typeof(List<>);
        //        var constructedListType = listType.MakeGenericType(type);
        //        var instance = Activator.CreateInstance(constructedListType) as IList;
        //        instance.Add(entitiesToUpsert);
        //        entities = instance;
        //    }
        //    else
        //    {
        //        entities = entitiesToUpsert as IEnumerable;
        //    }


        //    var tableName = contribType.GetTableName(type); //GetTableName
        //    var sbColumnList = new StringBuilder(null);
        //    var allProperties = contribType.TypePropertiesCache(type); //TypePropertiesCache(type);
        //    var keyProperties = contribType.KeyPropertiesCache(type);// KeyPropertiesCache(type).ToList();
        //    var computedProperties = contribType.ComputedPropertiesCache(type);// ComputedPropertiesCache(type);
        //    var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

        //    //added need to include key column for upsert
        //    var allPropertiesExceptComputed = allProperties.Except(computedProperties).ToList();

        //    var explicitKeyProperties = contribType.ExplicitKeyPropertiesCache(type); // ExplicitKeyPropertiesCache(type);
        //    if (keyProperties.Count == 0 && explicitKeyProperties.Count == 0)
        //        throw new ArgumentException("Entity must have at least one [Key] or [ExplicitKey] property");

        //    keyProperties.AddRange(explicitKeyProperties);

        //    var columns = allPropertiesExceptComputed.Select(x => x.Name).ToList();

        //    var dbConnectionType = db.GetType().Name;
        //    int result;
        //    switch (dbConnectionType)
        //    {
        //        case "SQLiteConnection":
        //            result = await db.ReplaceInto<T>(tableName, columns, entities, transaction, commandTimeout);
        //            break;
        //        case "MySqlConnection":
        //            result = await db.MySQLUpsert<T>(tableName, columns, entities, transaction, commandTimeout);
        //            break;
        //        default:
        //            throw new Exception($"No method found for database type: {dbConnectionType}");
        //    }
        //    return result;
        //}

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
        //private static async Task<int> UpsertAsync(this IDbConnection db,
        //                                          string tableName,
        //                                          IEnumerable<string> columns,
        //                                          IEnumerable<dynamic> records,
        //                                          IDbTransaction transaction = null,
        //                                          int? commandTimeout = null)
        //{
        //    var dbConnectionType = db.GetType().Name;
        //    int result;
        //    switch (dbConnectionType)
        //    {
        //        case "SQLiteConnection":
        //            result = await db.ReplaceInto(tableName, columns, records, transaction, commandTimeout);
        //            break;
        //        case "MySqlConnection":
        //            result = await db.MySQLUpsert(tableName, columns, re, transaction, commandTimeout);
        //            break;
        //        default:
        //            throw new Exception($"No method found for database type: {dbConnectionType}");
        //    }
        //    return result;
        //}

        private static async Task BulkUpdate(IDbConnection db,
                                             List<string> columns,
                                             string tableToUpdate,
                                             string tempTableName)
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

        private static List<PropertyInfo> GetAllColumns<T>()
        {
            var type = typeof(T);
            var contribType = typeof(SqlMapperExtensions);

            var allProperties = contribType.TypePropertiesCache(type);
            var computedProperties = contribType.ComputedPropertiesCache(type);// ComputedPropertiesCache(type);
            var keyProperties = contribType.KeyPropertiesCache(type);
            var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();
            return allPropertiesExceptKeyAndComputed;
        }
        /// <summary>
        /// Get the type. If the type is IEnumerable, get the containing type
        /// </summary>
        /// <param name="Tentity"></param>
        /// <returns></returns>
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

        private static async Task<int> MySQLUpsert(this IDbConnection db,
                                                   string tableName,
                                                   IEnumerable<string> columns,
                                                   List<string> parameterizedValues,
                                                   DynamicParameters parameters,
                                                   IDbTransaction transaction = null,
                                                   int? commandTimeout = null)
        {
            var newList = new List<string>();

            foreach (var c in columns)
            {
                newList.Add($"{c} = VALUES({c})");
            }

            var cmd = $"INSERT INTO {tableName} ({String.Join(",", columns)}) VALUES {String.Join(",", parameterizedValues)} ON DUPLICATE KEY UPDATE {String.Join(",", newList)}";
            return await db.ExecuteAsync(cmd, parameters, transaction, commandTimeout);
        }


        /// <summary>
        /// MySQL Database Specific Upsert Command
        /// Uses ON DUPLICATE KEY UPDATE
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="entitiesToUpsert"></param>
        /// <param name="columns"></param>
        /// <param name="tableName"></param>
        /// <param name="chunkSize"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <returns></returns>
        private static async Task<int> MySQLUpsertAsync<T>(this IDbConnection db,
                                                   IEnumerable<T> entitiesToUpsert,
                                                   IEnumerable<string> columns,
                                                   string tableName,
                                                   int chunkSize = 1000,
                                                   IDbTransaction transaction = null,
                                                   int? commandTimeout = null)
        {
            int result = 0;       
            
            foreach (var entityChunk in entitiesToUpsert.Chunk2(1000))
            {
                var upsertParameters = BuildInsertParameters(columns, entitiesToUpsert);
                var newList = new List<string>();

                foreach (var c in columns)
                {
                    newList.Add($"{c} = VALUES({c})");
                }

                var cmd = $"INSERT INTO {tableName} ({string.Join(",", columns)}) VALUES {string.Join(",", upsertParameters.ParameterizedInsertValues)} ON DUPLICATE KEY UPDATE {string.Join(",", newList)}";
                result += await db.ExecuteAsync(cmd, upsertParameters.DynamicParams, transaction, commandTimeout);
            }
            return result;

            //return await db.MySQLUpsert(tableName, columns, items, dynamicParams, transaction, commandTimeout);
        }

        private static async Task<int> ReplaceInto(this IDbConnection db,
                                       string intoTableName,
                                       IEnumerable<string> columns,
                                       IEnumerable<dynamic> entitiesToReplaceInto,
                                       IDbTransaction transaction = null,
                                       int? commandTimeout = null)
        {
            var valueSb = new StringBuilder();
            var inserts = new List<string>();
            var dynamicParams = new DynamicParameters();
            long i = 0;
            foreach (IDictionary<string, object> record in entitiesToReplaceInto)
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
                                                   IEnumerable<string> columns,
                                                   List<string> recordInserts,
                                                   DynamicParameters parameters,
                                                   IDbTransaction transaction,
                                                   int? commandTimeout = null)
        {
            var cmd = $"REPLACE INTO {tableName} ({String.Join(",", columns)}) VALUES {String.Join(",", recordInserts)}";
            return await db.ExecuteAsync(cmd, parameters, transaction, commandTimeout);
        }

        private static async Task<int> ReplaceInto<Tentity>(this IDbConnection db,
                                                            string tableName,
                                                            IEnumerable<string> columns,
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

        private static (DynamicParameters DynamicParams, List<string> ParameterizedInsertValues) BuildInsertParameters<T>(IEnumerable<string> columns, IEnumerable<T> entities)
        {
            var entityType = typeof(T);
            var dynamicParams = new DynamicParameters();
            int i = 0;
            var valueList = new List<string>();
            foreach (var e in entities)
            {
                var pList = new List<string>();
                foreach (var column in columns)
                {
                    var value = entityType.GetProperty(column)?.GetValue(e, null);
                    string p1 = $"P{i}";
                    dynamicParams.Add(p1, value);
                    pList.Add($"@{p1}");
                    i++;
                }
                valueList.Add($"({string.Join(",", pList)})");
            }
            return (dynamicParams, valueList);
        }
    }

}
