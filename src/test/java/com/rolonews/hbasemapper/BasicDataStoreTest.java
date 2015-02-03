package com.rolonews.hbasemapper;
import com.google.common.base.Function;
import com.rolonews.hbasemapper.annotations.HValidate;
import com.rolonews.hbasemapper.annotations.Column;
import com.rolonews.hbasemapper.annotations.Table;
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

        when(connection.isTableAvailable(TableName.valueOf("Person"))).thenReturn(true);
        when(connection.getTable(TableName.valueOf("Person"))).thenReturn(tableInterface);

    }

    @After
    public void tearDown() throws IOException {
        verify(tableInterface).close();
    }

    @Test
    public void testPutObject() throws IOException {

        DataStore dataStore = DataStoreFactory.getDataStore(connection);
        Person person = new Person();
        person.id = "someId";
        person.name = "Peter";
        person.age = 13;


        dataStore.put(person,Person.class);
        verify(tableInterface).put(putCaptor.capture());
        Put put = putCaptor.getValue().get(0);

        assertTrue(put.has(Bytes.toBytes("info"),Bytes.toBytes("name")));
        assertTrue(put.has(Bytes.toBytes("info"), Bytes.toBytes("age")));
        assertArrayEquals(put.get(Bytes.toBytes("info"), Bytes.toBytes("age")).get(0).getValue(), Bytes.toBytes(person.age));
        assertArrayEquals(put.get(Bytes.toBytes("info"), Bytes.toBytes("name")).get(0).getValue(), Bytes.toBytes(person.name));

    }

    @Test
    public void testPutObjectWithKey() throws IOException {
        DataStore dataStore = DataStoreFactory.getDataStore(connection);
        Person person = new Person();
        person.name = "Peter";
        person.age = 13;

        dataStore.put("someId",person);

        verify(tableInterface).put(putCaptor.capture());
        Put put = putCaptor.getValue().get(0);

        assertArrayEquals(put.getRow(),Bytes.toBytes("someId"));
    }

    @Test
    public void testPutBulkObjects() throws IOException {
        DataStore dataStore = DataStoreFactory.getDataStore(connection);

        Person person1 = new Person();
        person1.id = "someId";
        person1.name = "Peter";
        person1.age = 13;

        Person person2 = new Person();
        person2.id = "someId1";
        person2.name = "John";
        person2.age = 84;

        List<BasicDataStoreTest.Person> personsToInsert = Arrays.asList(person1,person2);

        dataStore.put(personsToInsert, BasicDataStoreTest.Person.class);

        verify(tableInterface).put(putCaptor.capture());

        List<Put> capturesPuts = putCaptor.getValue();

        assertEquals(personsToInsert.size(),capturesPuts.size());

        for(int i=0;i<personsToInsert.size();i++){
            Person person = personsToInsert.get(i);
            Put put  = capturesPuts.get(i);

            assertArrayEquals(Bytes.toBytes(person.id),put.getRow());
        }


    }

    @Test
    public void testPutBulkObjectsWithArbitraryKeys(){
        DataStore dataStore = DataStoreFactory.getDataStore(connection);

        Person person1 = new Person();
        person1.name = "Peter";
        person1.age = 13;

        Person person2 = new Person();
        person2.name = "John";
        person2.age = 84;

        Function<BasicDataStoreTest.Person,String> keyFunction = new Function<Person, String>() {
            @Nullable
            @Override
            public String apply(Person person) {
                return person.name;
            }
        };

        dataStore.put(keyFunction,Arrays.asList(person1,person2), BasicDataStoreTest.Person.class);

    }

    @Test
    public void testDeleteObjects() throws IOException {
        DataStore dataStore = DataStoreFactory.getDataStore(connection);

        Person person1 = new Person();
        person1.id = "someId";
        person1.name = "Peter";
        person1.age = 13;

        dataStore.delete("someId", BasicDataStoreTest.Person.class);

        verify(tableInterface).delete(deleteCaptors.capture());

        Delete capturedDelete = deleteCaptors.getValue().get(0);

        assertArrayEquals(Bytes.toBytes(person1.id),capturedDelete.getRow());

    }

    @Test
    public void testBulkDeletes()throws IOException{
        DataStore dataStore = DataStoreFactory.getDataStore(connection);

        Person person1 = new Person();
        person1.id = "someId";
        person1.name = "Peter";
        person1.age = 13;

        Person person2 = new Person();
        person2.id = "someId1";
        person2.name = "John";
        person2.age = 84;

        List<BasicDataStoreTest.Person> personsToDelete = Arrays.asList(person1,person2);
        dataStore.delete(Arrays.asList("someId","someId1"),BasicDataStoreTest.Person.class);

        verify(tableInterface).delete(deleteCaptors.capture());
        List<Delete> capturedDeletes = deleteCaptors.getValue();

        for (int i=0;i<personsToDelete.size();i++) {
            assertArrayEquals(Bytes.toBytes(personsToDelete.get(i).id),capturedDeletes.get(i).getRow());
        }

    }


    @Table(name = "Person", columnFamilies = {"info"}, rowKeyGenerator = PersonRowKeyGenerator.class )
    @HValidate(validator = PersonValidator.class)
    class Person {

        public String id;

        @Column(family = "info", qualifier = "name")
        public String name;

        @Column(family = "info", qualifier = "age")
        public int age;


    }



}
class PersonRowKeyGenerator implements Function<BasicDataStoreTest.Person,String>{

    @Override
    public String apply(BasicDataStoreTest.Person person) {
        return person.id;
    }
}
