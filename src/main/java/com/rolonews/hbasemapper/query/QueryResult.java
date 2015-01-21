package com.rolonews.hbasemapper.query;

/**
 *
 * Created by Sleiman on 16/12/2014.
 */
public class QueryResult<K,T> {

    public final K rowKey;
    public final T result;

    public QueryResult(K rowKey,T result){
        this.rowKey  =rowKey;
        this.result = result;
    }

}
