package com.rolonews.hbasemapper;

import com.rolonews.hbasemapper.query.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import rx.observers.TestSubscriber;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by Sleiman on 12/05/2015.
 */
public class BasicAsyncDataStoreTest extends BaseTest {

    private static final HConnection connection = BaseTest.getMiniClusterConnection();

    AsyncDataStore<Foo> asyncDataStore;

    @Before
    public void setup(){
        asyncDataStore = DataStoreFactory.getAsyncDataStore(Foo.class,connection);
    }


    @After
    public void tearDown() throws IOException {
        BaseTest.truncateHBaseTable("FooTrial");
    }

    @Test
    public void testAsyncQuery(){
        List<Foo> someFoos = getSomeFoos();
        asyncDataStore.put(someFoos);

        com.rolonews.hbasemapper.query.Query<Foo> queryBuilder =  com.rolonews.hbasemapper.query.Query.builder(Foo.class)
                .equals("job", "Programmer")
                .build();

        TestSubscriber<String> ts = new TestSubscriber<String>();
        Observable<Foo> fooObservable = asyncDataStore.getAsync(queryBuilder);

        fooObservable.map(new Func1<Foo, String>() {

            @Override
            public String call(Foo foo) {
                return foo.getName();
            }
        }).subscribe(ts);

        ts.awaitTerminalEvent();
        ts.assertNoErrors();
        ts.assertReceivedOnNext(Arrays.asList("Sleiman","Martin"));
        System.out.println("End of Test");

    }

    @Test
    public void testAsyncQueryResult() throws InterruptedException {
        List<Foo> someFoos = getSomeFoos();
        asyncDataStore.put(someFoos);

        com.rolonews.hbasemapper.query.Query<Foo> queryBuilder =  com.rolonews.hbasemapper.query.Query.builder(Foo.class)
                .equals("job", "Programmer")
                .build();

        TestSubscriber<String> ts = new TestSubscriber<String>();

        final Observable<QueryResult<String,Foo>> fooObservable = asyncDataStore.getAsQueryResultAsync(String.class, queryBuilder);

        fooObservable.map(new Func1<QueryResult<String,Foo>, String>() {
            @Override
            public String call(QueryResult<String, Foo> fooQueryResult) {
                return fooQueryResult.rowKey();
            }
        }) .subscribe(ts);

        ts.awaitTerminalEvent();
        ts.assertNoErrors();
        ts.assertReceivedOnNext(Arrays.asList("1_Sleiman", "3_Martin"));
    }

    @Test
    public void testAsyncGet() {
        List<Foo> someFoos = getSomeFoos();
        asyncDataStore.put(someFoos);
        TestSubscriber<String> ts = new TestSubscriber<String>();

        asyncDataStore.getAsync("1_Sleiman").map(new Func1<Foo, String>() {
            @Override
            public String call(Foo foo) {
                return foo.getName();
            }
        }).subscribe(ts);

        ts.awaitTerminalEvent();
        ts.assertNoErrors();
        ts.assertReceivedOnNext(Arrays.asList("Sleiman"));

    }

    @Test
    public void testAsyncPut() {
        List<Foo> someFoos = getSomeFoos();

        TestSubscriber<String> ts = new TestSubscriber<String>();
        asyncDataStore.putAsync(someFoos).map(new Func1<Put, String>() {
            @Override
            public String call(Put put) {
                return Bytes.toString(put.getRow());
            }
        }).subscribe(ts);

        ts.awaitTerminalEvent();
        ts.assertNoErrors();
        ts.assertReceivedOnNext(Arrays.asList("1_Sleiman","1_Peter","2_John","3_Martin"));

    }

    @Test
    public void testAsyncDelete(){
        List<Foo> someFoos = getSomeFoos();
        asyncDataStore.put(someFoos);

        TestSubscriber<Delete> ts = new TestSubscriber<Delete>();
        asyncDataStore.deleteAsync(Arrays.asList("1_Sleiman","3_Martin")).subscribe(ts);
        ts.awaitTerminalEvent();
        ts.assertNoErrors();

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

        Foo foo3 = Foo.getInstance();
        foo3.setId(3);
        foo3.setName("Martin");
        foo3.setAge(41);
        foo3.setJob("Programmer");


        foos.add(foo);
        foos.add(foo1);
        foos.add(foo2);
        foos.add(foo3);

        return foos;
    }


}
