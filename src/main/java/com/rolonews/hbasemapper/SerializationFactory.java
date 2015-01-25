package com.rolonews.hbasemapper;

import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.BasicObjectSerializer;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.ObjectSerializer;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * Created by sleimanjneidi on 24/01/2015.
 */
public class SerializationFactory {

    public static ObjectSerializer getSerializer(Object object){
        Preconditions.checkNotNull(object);
        String typeName = object.getClass().getName();


        if(String.class.getName().equals(typeName)
                || Integer.class.getName().equals(typeName) || "int".equals(typeName)
                || Double.class.getName().equals(typeName) || "double".equals(typeName)
                || Short.class.getName().equals(typeName) || "short".equals(typeName)
                || Boolean.class.getName().equals(typeName) || "boolean".equals(typeName)
                || Long.class.getName().equals(typeName) || "long".equals(typeName)
                || BigDecimal.class.getName().equals(typeName)
                || object instanceof ByteBuffer){

         return new BasicObjectSerializer();
        }


        throw new NotImplementedException();
    }

}
