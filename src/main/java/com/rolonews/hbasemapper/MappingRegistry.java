package com.rolonews.hbasemapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Sleiman on 25/01/2015.
 */

class MappingRegistry{

    private static final Map<Class<?>,EntityMapper<?>> mappedEntities = new ConcurrentHashMap<Class<?>, EntityMapper<?>>();

    public static <T> EntityMapper<T> getMapping(Class<T> clazz){
        return (EntityMapper<T>)mappedEntities.get(clazz);
    }

    public static <T> void register(EntityMapper<T> mapper){
        mappedEntities.put(mapper.clazz(),mapper);
    }
}
