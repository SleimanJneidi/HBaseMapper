package com.rolonews.hbasemapper;

import org.apache.hadoop.hbase.client.HConnection;

import java.util.List;

/**
 * Created by Sleiman on 10/12/2014.
 */
public class BasicDataStore extends DataStore {


    protected BasicDataStore(final HConnection connection) {
        super(connection);
    }

    @Override
    public void put(Object object) {

    }

    @Override
    public void put(List<?> objects) {

    }

    @Override
    public <T> void put(T key, Object object) {

    }

    @Override
    public <K, V> List<V> get(K key) {
        return null;
    }

    @Override
    public void delete(Object object) {

    }

    @Override
    public void delete(Object key, Class<?> clazz) {

    }
}
