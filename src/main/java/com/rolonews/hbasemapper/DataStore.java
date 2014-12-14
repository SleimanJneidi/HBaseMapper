package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ListenableFuture;
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

    public <K,T> Optional<T> get(K key, Class<T> clazz);

    public void delete(Object key,Class<?> clazz);

    public void delete(List<?> keys,Class<?> clazz);

}
