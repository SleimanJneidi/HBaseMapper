package com.rolonews.hbasemapper;

import org.apache.hadoop.hbase.client.HConnection;

import java.util.List;

/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public abstract class DataStore {

    private final HConnection connection;

    protected DataStore(HConnection connection) {
        this.connection = connection;
    }

    public static DataStore getInstance(final HConnection connection){
        return new BasicDataStore(connection);
    }

    public abstract void put(Object object);

    public abstract void put(List<?> objects);

    public abstract <T>void put(T key,Object object);

    public abstract <K,V> List<V> get(K key);

    public abstract void delete(Object object);

    public abstract void delete(Object key,Class<?> clazz);

    public HConnection getConnection(){
        return this.connection;
    }
}
