package com.rolonews.hbasemapper;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.HResultParser;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * Created by Sleiman on 11/12/2014.
 *
 */
public class BaseDataStoreIntegrationTest extends BaseTest {

    @Test
    public void testCanParseResultFromType() throws Exception{
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

        assertEquals(foo.getName(),foo1.getName());
        assertEquals(foo.getJob(),foo1.getJob());
        assertEquals(foo.getAge(),foo1.getAge());


    }

    @Test
    public void testCanGetObjectByKey() throws Exception{

        HConnection connection = BaseTest.getMiniClusterConnection();
        DataStore dataStore = DataStoreFactory.getDataStore(connection);

        Foo foo = new Foo();
        foo.setId(1);
        foo.setName("Sleiman");
        foo.setAge(12);
        foo.setJob("Jobless");

        dataStore.put(foo);

        Foo foo1 = dataStore.get("1_Sleiman",Foo.class).get();

        assertEquals(foo.getId(),foo1.getId());
        assertEquals(foo.getName(),foo1.getName());
        assertEquals(foo.getJob(),foo1.getJob());
        assertEquals(foo.getAge(),foo1.getAge());

    }



}
