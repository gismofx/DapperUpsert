# DapperUpsert
**NetStandard2.0 Library**
**Extension Methods for Easy Async UPSERT using Dapper and Dapper.Contrib**

## Usage
This library extends `IDBConnection` in the same fashion that Dapper and Dapper.Contrib do. If requires using attributes on your POCO classes specified in Dapper.Contrib

[Dapper.Contrib](https://github.com/DapperLib/Dapper.Contrib)

### Limitations
This currently works with SQLite and MYsql/MariaDB databases. MSsql and PostgreSQL are in the works, but will accept pull requests

In order to be DRY, and since we're relying on defining and adorning POCO classes using `Dapper.Contrib`, this library uses reflection to invoke the **private** methods in `SqlMapperExtensions`. This may not be the best practice and could break with newer releases of Dapper.Contrib.

Only tested with [ExplicitKey] attribute on primary key field in the POCO.

### Generic Example

```C#
var myRecords = FetchMyRecords(); //your code to get your records to Upsert
var result = await myDBConnection.UpsertAsyc(myRecords);
```
### Non-Generic Example

```C#
private List<string> GetColumnsToSync(Type entity)
  {
      var contribType = typeof(SqlMapperExtensions);

      var type = entity;

      var tableName = contribType.GetTableName(type); //GetTableName
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

      return allPropertiesExceptComputed.Select(x => x.Name).ToList();
  }

...

var myRecords = FetchMyRecords(); //your code to get your records to Upsert
var tableName = typeof(SqlMapperExtensions).GetTableName(myPocoClass);
var columns = GetColumnsToSync(myPocoClass);
var upsertResult = await RemoteDB.UpsertAsync(tableName, columnsToSync, myRecords);
```


## To Do
- Add Additional Database Support
- Add Tests for the private methods used
- Add Bulk Update
- Publish NuGet Package

