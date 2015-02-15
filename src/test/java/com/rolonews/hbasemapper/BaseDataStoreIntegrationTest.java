package com.rolonews.hbasemapper;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.rolonews.hbasemapper.exceptions.ColumnNotMappedException;
import com.rolonews.hbasemapper.mapping.MappingRegistry;
import com.rolonews.hbasemapper.query.HResultParser;
import com.rolonews.hbasemapper.query.IQuery;
import com.rolonews.hbasemapper.query.Query;
import com.rolonews.hbasemapper.query.QueryResult;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 *
 * Created by Sleiman on 11/12/2014.
 */
public class BaseDataStoreIntegrationTest extends BaseTest {

    private static final HConnection connection = BaseTest.getMiniClusterConnection();


    @After
    public void tearDown() throws IOException {

        if(connection.isTableAvailable(TableName.valueOf("FooTrial"))){
            HTableInterface table = connection.getTable("FooTrial");
            // truncate the table
            List<Delete> deletes = new ArrayList<Delete>();
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for(Result result: resultScanner){
                byte[] row = result.getRow();
                Delete delete = new Delete(row);
                deletes.add(delete);
            }
            resultScanner.close();
            table.delete(deletes);
            table.close();
        }
    }

    @Test
    public void testCanPutObjects() throws Exception{

        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);


        Foo foo = Foo.getInstance();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        Foo foo1 = Foo.getInstance();
        foo1.setId(2);
        foo1.setName("Sleiman");
        foo1.setAge(12);
        foo1.setJob("Jobless");

        dataStore.put(Arrays.asList(foo,foo1));
        assertEquals(2,rowCount(connection.getTable("FooTrial"),"info"));
    }

    @Test
    public void testQueryWithResult() throws Exception{

        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);


        Foo foo = Foo.getInstance();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        Foo foo1 = Foo.getInstance();
        foo1.setId(2);
        foo1.setName("Sleiman");
        foo1.setAge(12);
        foo1.setJob("Jobless");

        dataStore.put(Arrays.asList(foo, foo1));

        IQuery<Foo> query = Query.builder(Foo.class).build();

        List<QueryResult<String, Foo>> asQueryResult = dataStore.getAsQueryResult(String.class, query);
        QueryResult<String, Foo> first = asQueryResult.get(0);
        QueryResult<String, Foo> second = asQueryResult.get(1);

        assertEquals("1_Sleiman",first.rowKey());
        assertEquals("2_Sleiman",second.rowKey());



    }

    @Test
    public void testCanParseResultFromType() throws Exception {
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);

        Foo foo = Foo.getInstance();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        dataStore.put(foo);

        Result result = connection.getTable("FooTrial").get(new Get(Bytes.toBytes("1_Sleiman")));

        Supplier<Foo> creator = new Supplier<Foo>() {

            @Override
            public Foo get() {
                return Foo.getInstance();
            }

        };

        Foo foo1 = new HResultParser<Foo>(Foo.class,MappingRegistry.getMapping(Foo.class), Optional.of(creator)).valueOf(result);

        assertEquals(foo.getName(), foo1.getName());
        assertEquals(foo.getJob(), foo1.getJob());
        assertEquals(foo.getAge(), foo1.getAge());


    }

    @Test
    public void testCanGetObjectByKey() throws Exception {

        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);

        Foo foo = Foo.getInstance();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        dataStore.put(foo);

        Foo foo1 = dataStore.get("1_Sleiman").get();

        assertEquals(foo.getName(), foo1.getName());
        assertEquals(foo.getJob(), foo1.getJob());
        assertEquals(foo.getAge(), foo1.getAge());

    }

    @Test
    public void testCanDeleteObjectByKey() throws Throwable {
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);


        // put some data first
        Foo foo = Foo.getInstance();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        Foo foo1 = Foo.getInstance();
        foo1.setId(2);
        foo1.setName("Sleiman");
        foo1.setAge(12);
        foo1.setJob("Jobless");

        dataStore.put(foo);
        dataStore.put(foo1);

        HTableInterface tableInterface = connection.getTable("FooTrial");
        int rowCount = rowCount(tableInterface, "info");

        assertEquals(2, rowCount);

        dataStore.delete("1_Sleiman");

        rowCount = rowCount(tableInterface, "info");
        assertEquals(1, rowCount);

    }

    @Test
    public void testQueryByRowPrefix(){
        List<Foo> someFoos = getSomeFoos();

        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);

        dataStore.put(someFoos);
        Query<Foo> queryBuilder =  Query.builder(Foo.class).rowKeyPrefix("1_").build();
        List<Foo> results = dataStore.get(queryBuilder);
        assertTrue(results.size()==2);
    }

    @Test
    public void testQueryByEquals(){
        List<Foo> someFoos = getSomeFoos();
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
        dataStore.put(someFoos);

        Query<Foo> queryBuilder =  Query.builder(Foo.class).equals("age", 12).build();
        List<Foo> results = dataStore.get(queryBuilder);

        for (Foo result : results) {
            assertEquals(12,result.getAge());
        }
    }

    @Test
    public void testQueryCombination(){
        List<Foo> someFoos = getSomeFoos();
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
        dataStore.put(someFoos);

        Query<Foo> queryBuilder =  Query.builder(Foo.class).rowKeyPrefix("1_").equals("job", "Programmer")
                .build();
        List<Foo> results = dataStore.get(queryBuilder);

        for (Foo result : results) {
            assertEquals("Programmer",result.getJob());
        }
    }

    @Test(expected = ColumnNotMappedException.class)
    public void testShouldThrowExceptionIfFieldDoesNotExist(){
        List<Foo> someFoos = getSomeFoos();
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
        dataStore.put(someFoos);

        Query<Foo> queryBuilder =  Query.builder(Foo.class).rowKeyPrefix("1_").equals("someStupidField", "Programmer").build();
        List<Foo> results = dataStore.get(queryBuilder);

        for (Foo result : results) {
            assertEquals("Programmer",result.getJob());
            assertEquals(1,result.getId());
        }
    }

    @Test
    public void testQueryCombination1(){

        List<Foo> someFoos = getSomeFoos();
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
        dataStore.put(someFoos);

        Query<Foo> queryBuilder =  Query.builder(Foo.class).greaterThanOrEqaul("age", 12).build();
        List<Foo> results = dataStore.get(queryBuilder);

        assertEquals(someFoos.size(),results.size());
    }

    private List<Foo> getSomeFoos(){
        List<Foo> foos = new ArrayList<Foo>();

        Foo foo = Foo.getInstance();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Programmer");

        Foo foo1 = Foo.getInstance();
        foo1.setId(1);
        foo1.setName("Peter");
        foo1.setAge(12);
        foo1.setJob("Accountant");

        Foo foo2 = Foo.getInstance();
        foo2.setId(2);
        foo2.setName("John");
        foo2.setAge(13);
        foo2.setJob("Jobless");

        foos.add(foo);
        foos.add(foo1);
        foos.add(foo2);

        return foos;
    }


}
