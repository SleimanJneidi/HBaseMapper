package com.rolonews.hbasemapper;

import java.util.List;

/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public abstract class DataStore {


    private static DataStore dataStore;

    public static DataStore getInstance(){
        return dataStore;
    }

    public abstract void put(Object object);

    public abstract void put(List<?> objects);

    public abstract <T>void put(T key,Object object);

    public abstract <K,V> List<V> get(K key);

    public abstract void delete(Object object);

    public abstract void delete(Object key,Class<?> clazz);

}
