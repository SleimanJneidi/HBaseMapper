# HBase Mapper
##Overview
A simple object to HBase mapping library that simplifies the
access to HBase. Mapping can be done fluently or using annotations.

##Examples

<code>
class Foo{
    private String name;
    private int age;

    private Foo(){
    }
}
</code>
###Fluent Mapping

<code>
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
</code>
###Storing to HBase
<code>

 HConnection connection = get yourself a connection ...
 DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
 Foo foo = new Foo();
 dataStore.put(foo);

</code>
###Querying

<code>

IQuery<Foo> query =  Query.builder(Foo.class).startRow("prefix")
.stopRow("prefix~").limit(10).build();

 HConnection connection = get yourself a connection ...

 DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
 List<Foo> results = dataStore.get(query);

</code>


