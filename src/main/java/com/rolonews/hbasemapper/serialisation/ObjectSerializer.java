package com.rolonews.hbasemapper.serialisation;


/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public interface ObjectSerializer<T> {
	
    byte []serialize(T object, SerialisationManager serialisationManager);
    
    T deserialize(byte[] buffer, Class<T> clazz, SerialisationManager serialisationManager);
}
