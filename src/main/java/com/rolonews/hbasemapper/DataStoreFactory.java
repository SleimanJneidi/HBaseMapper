package com.rolonews.hbasemapper;

import org.apache.hadoop.hbase.client.HConnection;

import com.rolonews.hbasemapper.mapping.EntityMapper;

/**
 *
 * Created by Sleiman on 12/12/2014.
 *
 */
public abstract class DataStoreFactory {

    public static <T> DataStore<T> getDataStore(final Class<T> clazz, final HConnection connection){
        return new BasicDataStore<T>(connection,clazz,null);
    }

    public static <T> DataStore<T> getDataStore(final Class<T> clazz, final HConnection connection, final EntityMapper<T> mapper){
        return new BasicDataStore<T>(connection,clazz,mapper);
    }


    public static <T> AsyncDataStore<T> getAsyncDataStore(final Class<T> clazz,final HConnection connection){
        BasicDataStore<T> dataStore = new BasicDataStore<T>(connection,clazz,null);
        BasicAsyncDataStore<T> asyncDataStore = new BasicAsyncDataStore<T>(dataStore);
        return asyncDataStore;
    }

    public static <T> AsyncDataStore<T> getAsyncDataStore(final Class<T> clazz,final HConnection connection,final EntityMapper<T> mapper){
        BasicDataStore<T> dataStore = new BasicDataStore<T>(connection,clazz,mapper);
        BasicAsyncDataStore<T> asyncDataStore = new BasicAsyncDataStore<T>(dataStore);
        return asyncDataStore;
    }

}
