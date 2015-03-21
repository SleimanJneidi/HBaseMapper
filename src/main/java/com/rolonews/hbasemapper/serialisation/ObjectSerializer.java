package com.rolonews.hbasemapper.serialisation;


/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public interface ObjectSerializer {
    byte []serialize(Object object);
    <T>  T deserialize(byte[] buffer, Class<T> clazz);
}