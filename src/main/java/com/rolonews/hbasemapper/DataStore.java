package com.rolonews.hbasemapper;

import com.google.common.base.Optional;
import com.rolonews.hbasemapper.query.IQuery;
import com.rolonews.hbasemapper.query.QueryResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public interface DataStore<T> {

    public Put put(T object);

    public List<Put> put(List<T> objects);

    public Put put(Object key,T object);

    public <K> Optional<T> get(K key);

    public Delete delete(Object key);

    public List<Delete> delete(List<?> keys);

    public List<T> get(IQuery<T> query);

    public <K> List<QueryResult<K,T>> getAsQueryResult(Class<K> rowKeyClazz, IQuery<T> query);

}
