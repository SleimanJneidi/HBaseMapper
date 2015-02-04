package com.rolonews.hbasemapper.query;

/**
 *
 * Created by Sleiman on 16/12/2014.
 */
public class QueryResult<K,T> {

    private final K rowKey;
    private final T object;

    public QueryResult(K rowKey,T object){
        this.rowKey  =rowKey;
        this.object = object;
    }

    public K rowKey(){
        return this.rowKey;
    }

    public T object(){
        return this.object;
    }

}
