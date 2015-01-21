package com.rolonews.hbasemapper;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.HResultParser;

import com.rolonews.hbasemapper.exceptions.ColumnNotMappedException;
import com.rolonews.hbasemapper.query.Query;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 *
 * Created by Sleiman on 11/12/2014.
 */
public class BaseDataStoreIntegrationTest extends BaseTest {

    private static final HConnection connection = BaseTest.getMiniClusterConnection();


    @After
    public void tearDown() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(connection.getConfiguration());
        admin.disableTable("FooTrial");
        admin.deleteTable("FooTrial");
    }

    @Test
    public void testCanPutObjects() throws Exception{

        DataStore dataStore = DataStoreFactory.getDataStore(connection);


        Foo foo = new Foo();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        Foo foo1 = new Foo();
        foo1.setId(2);
        foo1.setName("Sleiman");
        foo1.setAge(12);
        foo1.setJob("Jobless");

        dataStore.put(Arrays.asList(foo,foo1),Foo.class);
        assertEquals(2,rowCount(connection.getTable("FooTrial"),"info"));
    }

    @Test
    public void testCanParseResultFromType() throws Exception {
        DataStore dataStore = DataStoreFactory.getDataStore(connection);

        Foo foo = new Foo();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        dataStore.put(foo);

        Result result = connection.getTable("FooTrial").get(new Get(Bytes.toBytes("1_Sleiman")));

        Supplier<Foo> creator = new Supplier<Foo>() {

            @Override
            public Foo get() {
                return new Foo();
            }

        };

        Foo foo1 = new HResultParser<Foo>(Foo.class, Optional.of(creator)).valueOf(result);

        assertEquals(foo.getName(), foo1.getName());
        assertEquals(foo.getJob(), foo1.getJob());
        assertEquals(foo.getAge(), foo1.getAge());


    }

    @Test
    public void testCanGetObjectByKey() throws Exception {

        DataStore dataStore = DataStoreFactory.getDataStore(connection);

        Foo foo = new Foo();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        dataStore.put(foo);

        Foo foo1 = dataStore.get("1_Sleiman", Foo.class).get();

        assertEquals(foo.getId(), foo1.getId());
        assertEquals(foo.getName(), foo1.getName());
        assertEquals(foo.getJob(), foo1.getJob());
        assertEquals(foo.getAge(), foo1.getAge());

    }

    @Test
    public void testCanDeleteObjectByKey() throws Throwable {
        DataStore dataStore = DataStoreFactory.getDataStore(connection);


        // put some data first
        Foo foo = new Foo();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        Foo foo1 = new Foo();
        foo1.setId(2);
        foo1.setName("Sleiman");
        foo1.setAge(12);
        foo1.setJob("Jobless");

        dataStore.put(foo);
        dataStore.put(foo1);

        HTableInterface tableInterface = connection.getTable("FooTrial");
        int rowCount = rowCount(tableInterface, "info");

        assertEquals(2, rowCount);

        dataStore.delete("1_Sleiman", Foo.class);

        rowCount = rowCount(tableInterface, "info");
        assertEquals(1, rowCount);

    }

    @Test
    public void testQueryByRowPrefix(){
        List<Foo> someFoos = getSomeFoos();
        DataStore dataStore = DataStoreFactory.getDataStore(connection);
        dataStore.put(someFoos,Foo.class);

        Query<Foo> queryBuilder =  Query.builder(Foo.class).rowKeyPrefix("1_").build();
        List<Foo> results = dataStore.get(queryBuilder);

        assertTrue(results.size()==2);
    }

    @Test
    public void testQueryByEquals(){
        List<Foo> someFoos = getSomeFoos();
        DataStore dataStore = DataStoreFactory.getDataStore(connection);
        dataStore.put(someFoos,Foo.class);

        Query<Foo> queryBuilder =  Query.builder(Foo.class).equals("age", 12).build();
        List<Foo> results = dataStore.get(queryBuilder);

        for (Foo result : results) {
            assertEquals(12,result.getAge());
        }
    }

    @Test
    public void testQueryCombination(){
        List<Foo> someFoos = getSomeFoos();
        DataStore dataStore = DataStoreFactory.getDataStore(connection);
        dataStore.put(someFoos,Foo.class);

        Query<Foo> queryBuilder =  Query.builder(Foo.class).rowKeyPrefix("1_").equals("job", "Programmer")
                .build();
        List<Foo> results = dataStore.get(queryBuilder);

        for (Foo result : results) {
            assertEquals("Programmer",result.getJob());
            assertEquals(1,result.getId());
        }
    }

    @Test(expected = ColumnNotMappedException.class)
    public void testShouldThrowExceptionIfFieldDoesNotExist(){
        List<Foo> someFoos = getSomeFoos();
        DataStore dataStore = DataStoreFactory.getDataStore(connection);
        dataStore.put(someFoos,Foo.class);

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
        DataStore dataStore = DataStoreFactory.getDataStore(connection);
        dataStore.put(someFoos,Foo.class);

        Query<Foo> queryBuilder =  Query.builder(Foo.class).greaterThanOrEqaul("age", 12).build();
        List<Foo> results = dataStore.get(queryBuilder);

        assertEquals(someFoos.size(),results.size());
    }





    private List<Foo> getSomeFoos(){
        List<Foo> foos = new ArrayList<Foo>();

        Foo foo = new Foo();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Programmer");

        Foo foo1 = new Foo();
        foo1.setId(1);
        foo1.setName("Peter");
        foo1.setAge(12);
        foo1.setJob("Accountant");

        Foo foo2 = new Foo();
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
