package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.client.HConnection;

import java.util.List;

/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public interface DataStore {

    public void put(Object object);

    public <T> void put(List<T> objects,Class<T> clazz);

    public void put(Object key,Object object);

    public <K,T> void put(Function<T,K> rowKeyFunction, List<T> objects, Class<T> clazz);

    public <K,T> T get(K key);

    public void delete(Object object);

    public void delete(Object key,Class<?> clazz);


}
