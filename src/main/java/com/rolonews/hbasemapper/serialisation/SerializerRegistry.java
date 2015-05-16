package com.rolonews.hbasemapper.serialisation;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.base.Preconditions;

/**
 * 
 * @author maamria
 *
 */
public final class SerializerRegistry {

	private final ConcurrentHashMap<Class<?>, ObjectSerializer<?>> serialisers;
	
	public SerializerRegistry(){
		serialisers = new ConcurrentHashMap<Class<?>, ObjectSerializer<?>>(8, 0.9f, 1);
	}
    
    /**
     * Adds a serialiser for class <code>clazz</code>.
     * @param clazz
     * @param serializer
     */
    public <T> void addSerializer(Class<T> clazz, ObjectSerializer<T> serializer){
    	Preconditions.checkNotNull(clazz);
    	Preconditions.checkNotNull(serializer);
    	serialisers.put(clazz, serializer);
    }
    
    public SerialisationManager getSerialisationManager(){
    	return new SerialisationManager(Collections.unmodifiableMap(serialisers));
    }
}
