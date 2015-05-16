package com.rolonews.hbasemapper.mapping;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Sleiman on 25/01/2015.
 */

public class MappingRegistry{

    private static final ConcurrentHashMap<Class<?>,EntityMapper<?>> mappedEntities = new ConcurrentHashMap<Class<?>, EntityMapper<?>>();

    @SuppressWarnings("unchecked")
	public static <T> EntityMapper<T> getMapping(Class<T> clazz){
        return (EntityMapper<T>)mappedEntities.get(clazz);
    }

    public static <T> void register(EntityMapper<T> mapper){
        mappedEntities.put(mapper.clazz(),mapper);
    }
}
