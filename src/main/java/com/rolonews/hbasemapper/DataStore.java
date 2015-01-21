package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.rolonews.hbasemapper.query.IQuery;

import java.util.List;

/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public interface DataStore {

    public <T> void put(T object);

    public <T> void put(List<T> objects,Class<T> clazz);

    public void put(Object key,Object object);

    public <K,T> void put(Function<T,K> rowKeyFunction, List<T> objects, Class<T> clazz);

    public <K,T> Optional<T> get(K key, Class<T> clazz);

    public void delete(Object key,Class<?> clazz);

    public void delete(List<?> keys,Class<?> clazz);

    public <T> List<T> get(IQuery<T> query);

}
