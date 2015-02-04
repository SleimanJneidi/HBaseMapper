package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.rolonews.hbasemapper.query.IQuery;
import com.rolonews.hbasemapper.query.QueryResult;

import java.util.List;

/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public interface DataStore<T> {

    public void put(T object);

    public void put(List<T> objects);

    public void put(Object key,T object);

    public <K> void put(Function<T,K> rowKeyFunction, List<T> objects);

    public <K> Optional<T> get(K key);

    public void delete(Object key);

    public void delete(List<?> keys);

    public List<T> get(IQuery<T> query);

    public <K> List<QueryResult<K,T>> getAsQueryResult(Class<K> rowKeyClazz, IQuery<T> query);

}
