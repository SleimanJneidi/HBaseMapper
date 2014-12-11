package com.rolonews.hbasemapper;
import com.google.common.base.Function;
import com.google.common.reflect.Reflection;
import com.rolonews.hbasemapper.annotations.HValidate;
import com.rolonews.hbasemapper.annotations.MColumn;
import com.rolonews.hbasemapper.annotations.MTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
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

import static org.mockito.Mockito.*;
/**
 * Created by Sleiman on 11/12/2014.
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicDataStoreTest extends BaseTest {

    @Mock
    HConnection connection;

    @Mock
    HTableInterface tableInterface;

    @Captor
    private ArgumentCaptor<Put> putCaptor;

    @Before
    public void setup() throws IOException {

        when(connection.isTableAvailable(TableName.valueOf("Person"))).thenReturn(true);
        when(connection.getTable(TableName.valueOf("Person"))).thenReturn(tableInterface);

    }

    @Test
    public void testPutObject() throws IOException {

        DataStore dataStore = DataStore.getInstance(connection);
        Person person = new Person();
        person.id = "someId";
        person.name = "Peter";
        person.age = 13;

        dataStore.put(person);
        verify(tableInterface).put(putCaptor.capture());

        Put put = putCaptor.capture();
        System.out.println(put.toJSON());
    }

    @Test
    public void testPutObjectWithKey(){
        DataStore dataStore = DataStore.getInstance(connection);
        Person person = new Person();
        person.name = "Peter";
        person.age = 13;

        dataStore.put("someId",person);

    }

    @Test
    public void testPutBulkObjects(){
        DataStore dataStore = DataStore.getInstance(connection);

        Person person1 = new Person();
        person1.id = "someId";
        person1.name = "Peter";
        person1.age = 13;

        Person person2 = new Person();
        person2.id = "someId1";
        person2.name = "John";
        person2.age = 84;

        dataStore.put(Arrays.asList(person1,person2), BasicDataStoreTest.Person.class);
    }

    @Test
    public void testPutBulkObjectsWithArbitraryKeys(){
        DataStore dataStore = DataStore.getInstance(connection);

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


    @MTable(name = "Person", rowKeys = {"id"}, columnFamilies = {"info"})
    @HValidate(validator = PersonValidator.class)
    class Person {

        private String id;

        @MColumn(family = "info", qualifier = "name")
        public String name;

        @MColumn(family = "info", qualifier = "age")
        public int age;


    }

}
