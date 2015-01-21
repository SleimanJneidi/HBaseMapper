package com.rolonews.hbasemapper.query;


import org.apache.hadoop.hbase.client.Scan;

public interface IQuery<T> {
    public Scan getScanner();
    public Class<T> getType();
}
