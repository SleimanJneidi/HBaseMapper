package com.rolonews.hbasemapper.query;

import org.apache.hadoop.hbase.client.Result;

/**
 *
 * Created by Sleiman on 11/12/2014.
 *
 */
public interface ResultParser<T> {
    T valueOf(Result result);
}
