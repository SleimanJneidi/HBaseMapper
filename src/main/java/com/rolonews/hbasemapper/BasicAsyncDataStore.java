package com.rolonews.hbasemapper;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.query.IQuery;
import com.rolonews.hbasemapper.query.QueryResult;
import org.apache.hadoop.hbase.client.*;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.List;

/**
 * Created by Sleiman on 12/05/2015.
 */
public class BasicAsyncDataStore<T> implements AsyncDataStore<T>{

    private final BasicDataStore<T> dataStore;

    protected BasicAsyncDataStore(BasicDataStore<T> dataStore){
        this.dataStore = dataStore;
    }

    @Override
    public Put put(T object) {
        return this.dataStore.put(object);
    }

    @Override
    public List<Put> put(List<T> objects) {
        return this.dataStore.put(objects);
    }

    @Override
    public Put put(Object key, T object) {
        return this.dataStore.put(key,object);
    }

    @Override
    public <K> Optional<T> get(K key) {
        return this.dataStore.get(key);
    }

    @Override
    public Delete delete(Object key) {
        return this.dataStore.delete(key);
    }

    @Override
    public List<Delete> delete(List<?> keys) {
        return this.dataStore.delete(keys);
    }

    @Override
    public List<T> get(IQuery<T> query) {
        return this.get(query);
    }

    @Override
    public <K> List<QueryResult<K, T>> getAsQueryResult(Class<K> rowKeyClazz, IQuery<T> query) {
        return this.getAsQueryResult(rowKeyClazz, query);
    }

    @Override
    public Observable<T> getAsync(final IQuery<T> query){

      Observable<T> observable =  Observable.create(new Observable.OnSubscribe<T>() {

            @Override
            public void call(final Subscriber<? super T> subscriber) {
                Preconditions.checkNotNull(query);
                final Scan scan = query.getScanner();

                Consumer<HTableInterface> applyScan = new Consumer<HTableInterface>() {
                    @Override
                    public void consume(HTableInterface hTableInterface) {
                        try {
                            ResultScanner resultScanner = hTableInterface.getScanner(scan);
                            for (Result result : resultScanner) {
                                T object = dataStore.resultParser.valueOf(result);
                                if (!subscriber.isUnsubscribed()) {
                                    subscriber.onNext(object);
                                }
                            }
                            resultScanner.close();
                            subscriber.onCompleted();

                        } catch (IOException e) {
                            subscriber.onError(e);
                        }
                    }
                };

                dataStore.operateOnTable(applyScan);
            }
        }).subscribeOn(Schedulers.io());

        return observable;
    }

    @Override
    public <K> Observable<QueryResult<K, T>> getAsQueryResultAsync(final Class<K> rowKeyClazz,final IQuery<T> query){
        Observable<QueryResult<K, T>> observable =  Observable.create(new Observable.OnSubscribe<QueryResult<K, T>>() {

            @Override
            public void call(final Subscriber<? super QueryResult<K, T>> subscriber) {
                Preconditions.checkNotNull(query);
                final Scan scan = query.getScanner();

                Consumer<HTableInterface> applyScan = new Consumer<HTableInterface>() {
                    @Override
                    public void consume(HTableInterface hTableInterface) {
                        try {
                            ResultScanner resultScanner = hTableInterface.getScanner(scan);
                            for (Result result : resultScanner) {
                                QueryResult<K, T> queryResult = dataStore.resultParser.valueAsQueryResult(rowKeyClazz, result);
                                if (!subscriber.isUnsubscribed()) {
                                    subscriber.onNext(queryResult);
                                }
                            }
                            resultScanner.close();
                            subscriber.onCompleted();

                        } catch (IOException e) {
                            subscriber.onError(e);
                        }
                    }
                };

                dataStore.operateOnTable(applyScan);
            }
        }).subscribeOn(Schedulers.io());

        return observable;
    }

    @Override
    public <K> Observable<T> getAsync(final K key){
        Observable<T> observable =  Observable.create(new Observable.OnSubscribe<T>() {

            @Override
            public void call(Subscriber<? super T> subscriber) {
                try {
                    Optional<T> result = dataStore.get(key);
                    if(result.isPresent()) {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onNext(result.get());
                        }
                    }
                    subscriber.onCompleted();
                } catch (Exception e) {
                    subscriber.onError(e);
                }
            }
        }).subscribeOn(Schedulers.io());
        return observable;
    }

    @Override
    public Observable<Put> putAsync(final List<T> objects){

        Observable<Put> observable = Observable.create(new Observable.OnSubscribe<Put>(){

            @Override
            public void call(Subscriber<? super Put> subscriber) {
                try {
                    List<Put> puts = dataStore.put(objects);
                    for (Put put : puts) {
                        if (!subscriber.isUnsubscribed()) {
                            subscriber.onNext(put);
                        }
                    }
                    subscriber.onCompleted();
                }catch (Exception exception){
                    subscriber.onError(exception);
                }

            }
        }).subscribeOn(Schedulers.io());
        return observable;
    }

    @Override
    public Observable<Delete> deleteAsync(final List<?> keys){
        final Observable<Delete> observable = Observable.create(new Observable.OnSubscribe<Delete>() {

            @Override
            public void call(Subscriber<? super Delete> subscriber) {
                try{
                    List<Delete> deletes = dataStore.delete(keys);
                    for (Delete delete : deletes) {
                        if(!subscriber.isUnsubscribed()){
                            subscriber.onNext(delete);
                        }
                    }
                    subscriber.onCompleted();
                }catch (Exception e){
                    subscriber.onError(e);
                }
            }

        }).subscribeOn(Schedulers.io());
        return observable;
    }
}
