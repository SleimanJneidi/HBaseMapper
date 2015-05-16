package com.rolonews.hbasemapper;
import com.google.common.base.Function;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
/**
 *
 * Created by Sleiman on 11/12/2014.
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicDataStoreTest extends BaseTest {

    @Mock
    HConnection connection;

    @Mock
    HTableInterface tableInterface;

    @Captor
    private ArgumentCaptor<List<Put>> putCaptor;

    @Captor
    private ArgumentCaptor<List<Delete>> deleteCaptors;

    @Before
    public void setup() throws IOException {

        when(connection.isTableAvailable(TableName.valueOf("FooTrial"))).thenReturn(true);
        when(connection.getTable(TableName.valueOf("FooTrial"))).thenReturn(tableInterface);

    }

    @After
    public void tearDown() throws IOException {
        verify(tableInterface).close();
    }

    @Test
    public void testPutObject() throws IOException {

        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
        Foo foo = Foo.getInstance();
        foo.setId(10);
        foo.setName("Peter");
        foo.setAge(13);
       
        dataStore.put(foo);
        verify(tableInterface).put(putCaptor.capture());
        Put put = putCaptor.getValue().get(0);

        assertTrue(put.has(Bytes.toBytes("info"),Bytes.toBytes("name")));
        assertTrue(put.has(Bytes.toBytes("info"), Bytes.toBytes("age")));
        assertArrayEquals(put.get(Bytes.toBytes("info"), Bytes.toBytes("age")).get(0).getValue(), Bytes.toBytes(foo.getAge()));
        assertArrayEquals(put.get(Bytes.toBytes("info"), Bytes.toBytes("name")).get(0).getValue(), Bytes.toBytes(foo.getName()));

    }

    @Test
    public void testPutObjectWithKey() throws IOException {
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);
        Foo foo = Foo.getInstance();
        foo.setName("Peter");
        foo.setAge(13);

        dataStore.put("10",foo);

        verify(tableInterface).put(putCaptor.capture());
        Put put = putCaptor.getValue().get(0);

        assertArrayEquals(put.getRow(),Bytes.toBytes("10"));
    }

    @Test
    public void testPutBulkObjects() throws IOException {
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);

        Foo foo1 = Foo.getInstance();
        foo1.setId(10);
        foo1.setName("Peter");
        foo1.setAge(13);

        Foo foo2 = Foo.getInstance();
        foo2.setId(11);
        foo2.setName("John");
        foo2.setAge(84);

        List<Foo> foosToInsert = Arrays.asList(foo1,foo2);

        dataStore.put(foosToInsert);

        verify(tableInterface).put(putCaptor.capture());

        List<Put> capturesPuts = putCaptor.getValue();

        assertEquals(foosToInsert.size(),capturesPuts.size());

        for(int i=0;i<foosToInsert.size();i++){
            Foo foo = foosToInsert.get(i);
            Put put  = capturesPuts.get(i);

            assertArrayEquals(Bytes.toBytes(foo.getId()+"_" + foo.getName()),put.getRow());
        }


    }

    @Test
    public void testDeleteObjects() throws IOException {
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);

        Foo foo1 = Foo.getInstance();
        foo1.setId(11);
        foo1.setName("Peter");
        foo1.setAge(13);

        dataStore.delete(11);

        verify(tableInterface).delete(deleteCaptors.capture());

        Delete capturedDelete = deleteCaptors.getValue().get(0);

        assertArrayEquals(Bytes.toBytes(foo1.getId()),capturedDelete.getRow());

    }

    @Test
    public void testBulkDeletes()throws IOException{
        DataStore<Foo> dataStore = DataStoreFactory.getDataStore(Foo.class,connection);

        Foo foo1 = Foo.getInstance();
        foo1.setId(11);
        foo1.setName("Peter");
        foo1.setAge(13);

        Foo foo2 = Foo.getInstance();
        foo2.setId(12);
        foo2.setName("John");
        foo2.setAge(84);

        List<Foo> FoosToDelete = Arrays.asList(foo1,foo2);
        dataStore.delete(Arrays.asList(11, 12));

        verify(tableInterface).delete(deleteCaptors.capture());
        List<Delete> capturedDeletes = deleteCaptors.getValue();

        for (int i=0;i<FoosToDelete.size();i++) {
            assertArrayEquals(Bytes.toBytes(FoosToDelete.get(i).getId()),capturedDeletes.get(i).getRow());
        }

    }

}
