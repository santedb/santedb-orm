# santedb-orm
SanteDB High-Performance Object-Relational Model

## About
The santedb-orm module provides a high-performance ORM for SanteDB similar to other ORM solutions such as Entity Framework, Linq-to-SQL, etc.

Originally SanteDB/OpenIZ used Entity Framework for interaction with the underlying database, however due to the complexity of SanteDB's 
RIM based data store, and the need to support not only SQL Server, but SQLite, PostgreSQL, and FirebirdSQL it was determined that an
alternate needed to be found and/or developed.

The team investigated several lightweight ORM solutions, however these:
1. Did not support LINQ
2. Did not provide an easy way to manually override common query patterns
3. Did not provide a mechanism for expressing foreign key relationships and/or special columns (encryption, indexing, etc.)

SanteDB-ORM does not provide facilities for re-constructing a database (code-first), rather it provides a mechanism for expressing
an existing relational database structure as .NET objects.

## Providers

The SanteDB-ORM layer has three providers which can be used for data access:

1. PostgreSQL - Supporting common PostgreSQL functions including fuzzystrmatch functions, ilike, etc.
1. FirebirdSQL - Supporting embedded databases to be used for unit-testing and development
1. SQLite - Supporting connections to SQLite databases

## Using the ORM

The ORM is used by creating a IDbProvider instance and a DataContext. For example, to connect to a Firebird database:

```
var provider = new FirebirdSQLProvider() { ConnectionString = "..." };
using(var context = new DataContext(provider, provider.GetWriteConnection())
{
  context.Open();
}
```

Once a context is created, you can insert, update and delete objects which have been mapped to the database using the appropriate methods:

```
var obj = new MappedDatabaseObject();
obj = context.Insert(obj); // Inserts the object into the database returning any auto-generated values from the DB

obj.PropertyName = "updated!";
obj = context.Update(obj); // Updates the object

context.Delete(obj); // removes the object
context.Delete<MappedDatabaseObject>(o=>o.Id == obj.Id); // Alternate syntax to DELETE FROM x WHERE 
```

Queries can be executed on the context using LINQ, for example, to get all object of type MappedDatabaseObject:

```
var results = context.Query<MappedDatabaseObject>(o => o.Id < 100);
```

The query is mapped to a SqlStatement object and executed as SQL. It is important to note that the variable ```results``` is an OrmResultSet<MappedDatabaseObject> instance, and your query is not executed until a scalar operation (example: Count) is executed or until the result set is loaded using ```foreach``` or ```ToList```. This means that Take(), Skip(), and Keys() aren't directly executed.

You can also construct a query manually using the SqlStatement class:

```
var stmt = context.CreateSqlStatement<MappedDatabaseObject>()
  .SelectFrom()
  .Where(o=>o.Id < 100);
```

### Composite Results

There are times when you wish to query from multiple tables defined by different classes. For example, if Foo is associated with Bar, we may want to load a dataset which is a composite of Foo and Bar. This is done with the CompositeResult<T> class.

```
[Table("foo_tbl")]
public class Foo {

  [Column("foo_id")]
  public int Id { get; set; }

  [Column("name")]
  public string Name { get; set; }
  
  [Column("bar_id"), ForeignKey(typeof(Bar), nameof(Bar.Id))]
  public int BarId { get; set; }
}

[Table("bar_tbl")]
public class Bar {

  [Column("bar_id")]
  public int Id { get; set; }
  
  [Column("bar_type")]
  public string Type { get; set; }
}

...

using(var context = new DataContext(provider, provider.GetReadonlyConnection()) {
  // Build the sql statement
  var stmt = context.CreateSqlStatement()
    .SelectFrom(typeof(Foo), typeof(Bar))
    .AutoJoin<Foo,Bar>()
    .Where<Foo>(o => o.Id < 100);
  foreach(var result in context.Query<CompositeResult<Foo, Bar>>(stmt))
    Console.WriteLine("Foo={0}, Bar={1}", result.Object1.Name, result.Object2.Type);
}
```

## Mapping to the DB

In order to map an object model to a database, a series of plain old C# classes are created and annotated with the SanteDB-ORM
attributes. These attributes are used to perform inserts, updates, and queries.

### Tables

The table attribute is used to identify a mapping to the underlying data schema for a table, for example, to map the class Foo to table foo_tbl:

```
[Table("foo_tbl")]
public class Foo {
}
```

### Columns

The column attribute is used to map a property from the .NET object to the underlying data structure. For example, to map the property Id to column foo_id in table foo_tbl:

```
[Table("foo_tbl")]
public class Foo {

  [Column("foo_id")]
  public int Id { get; set; }
}
```

### Foreign Keys

Certain properties can be linked together to create foreign key references. These attributes help the SqlStatement builder understand how joins should be created. 

For example, to link the column BarId to Foo.Id:

```
[Table("foo_tbl")]
public class Foo {

  [Column("foo_id")]
  public int Id { get; set; }
  
  [Column("bar_id"), ForeignKey(typeof(Bar), nameof(Bar.Id))]
  public int BarId { get; set; }
}

[Table("bar_tbl")]
public class Bar {

  [Column("bar_id")]
  public int Id { get; set; }
}
```

When joining across associative tables (many to many relationships) the AssociativeTable attribute is used. For example, if Foo and Bar have an associative entity FooBarAssociation and you want to traverse from Foo to Bar (have the Linq builder build this) then:

```
[Table("foo_tbl"), AssociativeTable(typeof(Bar), typeof(FooBarAssoc))]
public class Foo {

  [Column("foo_id")]
  public int Id { get; set; }
  
}

[Table("bar_tbl")]
public class Bar {

  [Column("bar_id")]
  public int Id { get; set; }
}

[Table("foo_bar_assoc_tbl")]
public class FooBarAssoc {
  
  [Column("foo_id"), ForeignKey(typeof(Foo), nameof(Foo.Id))]
  public int FooId { get; set; }
  
  [Column("bar_id"), ForeignKey(typeof(Bar), nameof(Bar.Id))]
  public int BarId { get; set; }
  
}
```

### Special Attributes

Additional special attributes can be used to identify extra attributes of an object. 

#### Primary Key Fields

The primary key attribute is used to denote property(ies) which make up the primary key in the database. This is used to create the apporpriate join coditions and update statements.

#### Auto Generated Fields

Auto-generated fields such as UUIDs, TIMESTAMP, SERIAL, or other fields where the database layer generates the data are annotated with [AutoGenerated]. For example, if the ID of foo were generated by the db layer:

```
[Table("foo_tbl")]
public class Foo {
  [Column("foo_id"), PrimaryKey, AutoGenerated]
  public Id { get; set; }
}
```

The values of auto-generated properties are hydrated upon insert, for example:

```
var foo = new Foo();
using(var conn = new NpgsqlConnection())
using(var context = new DataContext(new PostgreSQLProvider(), conn) {
  context.Open();
  var foo = context.Insert(foo);
  Console.Writeline("Foo was created with ID#{0}", foo.Id);
}
```

#### Join Filter 

Join filters are used when the query layer is creating a SqlStatement from a LINQ expression and is used to append a filter onto the join condition. For example, if we want to only fetch Bar instances where Active=True:

```
[Table("foo_tbl")]
public class Foo {

  [Column("foo_id")]
  public int Id { get; set; }
  
  [Column("bar_id"), ForeignKey(typeof(Bar), nameof(Bar.Id))]
  [JoinFilter(PropertyName = nameof(Bar.Active), Value = true)]
  public int BarId { get; set; }
}

[Table("bar_tbl")]
public class Bar {

  [Column("bar_id")]
  public int Id { get; set; }
  
  [Column("is_active")]
  public bool Active { get; set; }
}
```

#### Not Null Values

The purpose of the [NotNull] attribute is to perform application null checking prior to persisting the data to the database (of course, the RDBMS will enforce any NOT NULL constraints in addition to this validation). 

#### Secret Fields

Fields which are marked with [Secret] are those which can be persisted from the ORM layer, however should not appear in query results. Things like passwords, hashes, or others are candidates for this use. For example, if we don't want user's password hashes being returned:

```
[Table("user")]
public class User {

  [Column("user_id"), AutoGenerated, PrimaryKey]
  public int Id { get; set; }
  
  [Column("user_name"), NotNull]
  public string UserName { get; set; }
  
  [Column("user_password"), NotNull, Secret]
  public string PasswordHash { get; set; }
  
}
```
