package com.rolonews.hbasemapper;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.commons.lang.NotImplementedException;

/**
 * Created by Sleiman on 24/01/2015.
 */
public class SerializationFactory {

    public static ObjectSerializer getSerializer(Class<?> clazz){
        Preconditions.checkNotNull(clazz);
        String typeName = clazz.getName();


        if(String.class.getName().equals(typeName)
                || Integer.class.getName().equals(typeName) || "int".equals(typeName)
                || Double.class.getName().equals(typeName) || "double".equals(typeName)
                || Short.class.getName().equals(typeName) || "short".equals(typeName)
                || Boolean.class.getName().equals(typeName) || "boolean".equals(typeName)
                || Long.class.getName().equals(typeName) || "long".equals(typeName)
                || BigDecimal.class.getName().equals(typeName)
                || ByteBuffer.class.isAssignableFrom(clazz)){

            return new BasicObjectSerializer();
        }
        throw new NotImplementedException();
    }
    public static ObjectSerializer getSerializer(Object object){
        Preconditions.checkNotNull(object);
        Class<?> type = object.getClass();
        ObjectSerializer serializer = getSerializer(type);
        return serializer;
    }

}
