package com.rolonews.hbasemapper;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.HResultParser;
import com.sun.rowset.internal.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * Created by Sleiman on 11/12/2014.
 */
public class BaseDataStoreIntegrationTest extends BaseTest {

    @Test
    public void testCanParseResultFromType() throws Exception {
        HConnection connection = BaseTest.getMiniClusterConnection();
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

        HConnection connection = BaseTest.getMiniClusterConnection();
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
        HConnection connection = BaseTest.getMiniClusterConnection();
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


}
