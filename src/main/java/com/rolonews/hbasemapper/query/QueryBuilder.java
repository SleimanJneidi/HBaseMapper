package com.rolonews.hbasemapper.query;

import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.BasicObjectSerializer;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.ObjectSerializer;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;

/**
 *
 * Created by Sleiman on 14/12/2014.
 *
 */
public class QueryBuilder<T> {

    private final Class<T> clazz;
    private final Scan scanner;

    public QueryBuilder(Builder<T> builder){
        this.clazz = builder.clazz;
        this.scanner = builder.scanner;
    }

    public Scan getScanner(){
        return this.scanner;
    }

    public Class<T> getType(){
        return this.clazz;
    }

    public static class Builder<T>{

        private final Class<T> clazz;
        private final Scan scanner;
        private final ObjectSerializer serializer;

        public Builder(Class<T> clazz){
            this.clazz = clazz;
            this.scanner = new Scan();
            this.serializer = new BasicObjectSerializer();
        }

        public Builder<T> rowKeyPrefix(Object value){
            byte[] prefix = serializer.serialize(value);
            Filter filter = new PrefixFilter(prefix);
            scanner.setFilter(filter);
            return this;
        }

        public Builder<T> take(int limit,int offset){
            return this;
        }

        public Builder<T> equals(String field,Object value){
            return this;
        }

        public QueryBuilder<T> build(){
            return new QueryBuilder<T>(this);
        }

    }
}
