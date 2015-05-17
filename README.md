# HBase Mapper
##Overview
A simple object to HBase mapping API that simplifies the
access to HBase. You map Java classes to HBase table using a
simple and fluent API

##Examples

```
class Foo{
    private String name;
    private int age;
    private Foo(){
    }
}
```
###Fluent Mapping

```
EntityMapper<Foo> mapper = FluentEntityMapper.builder(Foo.class, "tableName")
        .withRowKeyGenerator(new Function<Foo, String>() {
            @Override
            public String apply(Foo input) {
               return input.name + (Long.MAX_VALUE - System.currentTimeMillis());
              }
          })
         .withColumnQualifier("columnFamilyName", "columnQualifierName", "name")
         .withColumnQualifier("columnFamilyName", "columnQualifierNameAge", "age")
         .build();
         .register();
```
###Storing to HBase
```
 HConnection connection = get yourself a connection ...
 DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
 Foo foo = new Foo();
 dataStore.put(foo);
```
###Querying

```
IQuery<Foo> query =  Query.builder(Foo.class).startRow("prefix")
.stopRow("prefix~").limit(10).build();

 HConnection connection = get yourself a connection ...

 DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
 List<Foo> results = dataStore.get(query);
```

###Async Data stores
HBaseMapper allows you to query HBase asynchronously using RxJava
```

AsyncDataStore<Foo> async=DataStoreFactory.getAsyncDataStore(Foo.class,connection);
Observable<Foo> fooObservable = asyncDataStore.getAsync(query);
fooObservable.subscribe(new Action1<Foo>() {
            @Override
            public void call(Foo foo) {
                // do something useful with foo
            }
        });

```
With Java 8, This becomes less verbose and more flexible

```
fooObservable.filter(foo->foo.name().startsWith("HBase"))
              .map(foo-> foo.name())
              .subscribe(System.out::println)

```
