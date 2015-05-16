package com.rolonews.hbasemapper;

import com.rolonews.hbasemapper.query.IQuery;
import com.rolonews.hbasemapper.query.QueryResult;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import rx.Observable;

import java.util.List;

/**
 * Created by Sleiman on 15/05/2015.
 */
public interface AsyncDataStore<T> extends DataStore<T> {
    Observable<T> getAsync(final IQuery<T> query);
    <K> Observable<QueryResult<K, T>> getAsQueryResultAsync(final Class<K> rowKeyClazz,final IQuery<T> query);
    <K> Observable<T> getAsync(final K key);
    Observable<Put> putAsync(final List<T> objects);
    Observable<Delete> deleteAsync(final List<?> keys);
}
