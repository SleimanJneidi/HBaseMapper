package com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler;


/**
 * Created by Sleiman on 09/12/2014.
 *
 */
public interface ObjectSerializer {
    byte []serialize(Object object);
    Object deserialize(byte[] buffer);
}