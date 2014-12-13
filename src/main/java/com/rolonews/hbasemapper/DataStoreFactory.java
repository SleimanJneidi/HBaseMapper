package com.rolonews.hbasemapper;

import org.apache.hadoop.hbase.client.HConnection;

/**
 *
 * Created by Sleiman on 12/12/2014.
 *
 */
public abstract class DataStoreFactory {

    public static DataStore getDataStore(final HConnection connection){
        return new BasicDataStore(connection);
    }
}
