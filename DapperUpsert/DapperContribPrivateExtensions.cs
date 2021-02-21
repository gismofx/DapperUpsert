using System;
using System.Collections.Generic;
using System.Reflection;
using Dapper;
using Dapper.Contrib.Extensions;
using System.Data;

namespace Dapper.Contrib.Extensions.Upsert
{
    public static class DapperContribPrivateExtensions
    {

        public static bool Call<Treturn>(this Type o, string methodName, bool staticMethod, out Treturn output, params object[] args)
        {
            output = default(Treturn);
            var type = o.GetType();
            var mi = o.GetMethod(methodName, System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            if (mi != null)
            {
                output = (Treturn)mi.Invoke(staticMethod?null:o, args);
            }
            return true;
        }

        public static string GetTableName(this Type sqlMapperExtensions, object tableModel)
        {
            string tableName;
            var result = sqlMapperExtensions.Call<string>("GetTableName", true, out tableName, tableModel);
            return tableName;
        }

        public static List<PropertyInfo> TypePropertiesCache(this Type sqlMapperExtensions, object tableModel)
        {
            List<PropertyInfo> myList;
            var result = sqlMapperExtensions.Call<List<PropertyInfo>>("TypePropertiesCache", true, out myList, tableModel);
            return myList;
        }

        public static List<PropertyInfo> KeyPropertiesCache(this Type sqlMapperExtensions, object tableModel)
        {
            List<PropertyInfo> myList;
            var result = sqlMapperExtensions.Call<List<PropertyInfo>>("KeyPropertiesCache", true, out myList, tableModel);
            return myList;
        }

        public static List<PropertyInfo> ExplicitKeyPropertiesCache(this Type sqlMapperExtensions, object tableModel)
        {
            List<PropertyInfo> myList;
            var result = sqlMapperExtensions.Call<List<PropertyInfo>>("ExplicitKeyPropertiesCache", true, out myList, tableModel);
            return myList;
        }

        public static List<PropertyInfo> ComputedPropertiesCache(this Type sqlMapperExtensions, object tableModel)
        {
            List<PropertyInfo> myList;
            var result = sqlMapperExtensions.Call<List<PropertyInfo>>("ComputedPropertiesCache", true, out myList, tableModel);
            return myList;
        }

        public static ISqlAdapter GetFormatter(this Type sqlMapperExtensions, IDbConnection connection)
        {
            ISqlAdapter adapter;
            var result = sqlMapperExtensions.Call<ISqlAdapter>("GetFormatter", true, out adapter);
            return adapter;
        }

        /*
        /// <summary>
        /// Attempts to execute a function and provide the result value against the provided source object even if it is private and/or static. Just make sure to provide the correct BindingFlags to correctly identify the function.
        /// </summary>
        /// <typeparam name="TReturn">The expected return type of the private method.</typeparam>
        /// <param name="type">The object's Type that contains the private method.</param>
        /// <param name="source">The object that contains the function to invoke. If looking for a static function, you can provide "null".</param>
        /// <param name="methodName">The name of the private method to run.</param>
        /// <param name="flags">Binding flags used to search for the function. Example: (BindingFlags.NonPublic | BindingFlags.Static) finds a private static method.</param>
        /// <param name="output">The invoked function's return value.</param>
        /// <param name="methodArgs">The arguments to pass into the private method.</param>
        /// <returns>Returns true if function was found and invoked. False if function was not found.</returns>
        private static bool TryInvokeMethod<TReturn>(Type type, object source, string methodName, BindingFlags flags, out TReturn output, params object[] methodArgs)
        {
            var method = type.GetMethod(methodName, flags);
            if (method != null)
            {
                output = (TReturn)method.Invoke(source, methodArgs);
                return true;
            }

            // Perform some recursion to walk the inheritance. 
            if (type.BaseType != null)
            {
                return TryInvokeMethod(type.BaseType, source, methodName, flags, out output, methodArgs);
            }

            output = default(TReturn);
            return false;
        }
        */

    }
}
