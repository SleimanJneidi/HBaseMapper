package com.rolonews.hbasemapper;

import org.apache.hadoop.hbase.client.HConnection;

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

}
