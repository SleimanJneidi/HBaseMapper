package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.client.HConnection;

import java.util.List;

/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public abstract class DataStore {

    protected final HConnection connection;

    protected DataStore(HConnection connection) {
        this.connection = connection;
    }

    public static DataStore getInstance(final HConnection connection){
        return new BasicDataStore(connection);
    }

    public abstract void put(Object object);

    public abstract <T> void put(List<T> objects,Class<T> clazz);

    public abstract void put(Object key,Object object);

    public abstract <K,T> void put(Function<T,K> rowKeyFunction, List<T> objects, Class<T> clazz);

    public abstract <K,T> T get(K key);

    public abstract void delete(Object object);

    public abstract void delete(Object key,Class<?> clazz);

    public HConnection getConnection(){
        return this.connection;
    }
}
