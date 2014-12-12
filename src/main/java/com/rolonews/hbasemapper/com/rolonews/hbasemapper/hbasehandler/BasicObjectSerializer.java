package com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 *
 * Created by Sleiman on 09/12/2014.
 *
 */
public class BasicObjectSerializer implements ObjectSerializer{

    @Override
    public byte[] serialize(Object object) {
        Preconditions.checkNotNull(object);
        String typeName = object.getClass().getName();

        if(String.class.getName().equals(typeName)) {
            return Bytes.toBytes((String) object);
        }
        if(Integer.class.getName().equals(typeName) || "int".equals(typeName)){
            return Bytes.toBytes((Integer)object);
        }
        if(Double.class.getName().equals(typeName) || "double".equals(typeName)) {
            return Bytes.toBytes((Double)object);
        }
        if(Short.class.getName().equals(typeName) || "short".equals(typeName)){
            return Bytes.toBytes((Short)object);
        }
        if(Boolean.class.getName().equals(typeName) || "boolean".equals(typeName)){
            return Bytes.toBytes((Boolean)object);
        }
        if(Long.class.getName().equals(typeName) || "long".equals(typeName)){
            return Bytes.toBytes((Long)object);
        }
        if(BigDecimal.class.getName().equals(typeName)){
            return Bytes.toBytes((BigDecimal)object);
        }
        if(object instanceof ByteBuffer){
            return Bytes.toBytes((ByteBuffer)object);
        }
        else{
            throw new NotImplementedException("Can't serialize object of type "+typeName);
        }
    }

    @Override
    public <T>  T deserialize(byte[] buffer,Class<T> clazz) {
        Preconditions.checkNotNull(buffer);
        Preconditions.checkNotNull(clazz);

        String typeName = clazz.getName();

        if (String.class.getName().equals(typeName)) {
            return (T)Bytes.toString(buffer);
        }
        if(Integer.class.getName().equals(typeName) || "int".equals(typeName)){
            return (T)Integer.valueOf(Bytes.toInt(buffer));
        }
        if(Double.class.getName().equals(typeName) || "double".equals(typeName)) {
            return (T)Double.valueOf(Bytes.toDouble(buffer));
        }
        if(Short.class.getName().equals(typeName) || "short".equals(typeName)){
            return (T)Short.valueOf(Bytes.toShort(buffer));
        }
        if(Boolean.class.getName().equals(typeName) || "boolean".equals(typeName)){
            return (T)Boolean.valueOf(Bytes.toBoolean(buffer));
        }
        if(Long.class.getName().equals(typeName) || "long".equals(typeName)){
            return (T)Long.valueOf(Bytes.toLong(buffer));
        }
        if(BigDecimal.class.getName().equals(typeName)){
            return (T) Bytes.toBigDecimal(buffer);
        }
        else{
            TypeToken typeToken = TypeToken.of(clazz);
            if(typeToken.isAssignableFrom(TypeToken.of(ByteBuffer.class).getType())){
                return (T)ByteBuffer.wrap(buffer);
            }
            throw new NotImplementedException("Can't serialize object of type "+typeName);
        }
    }
}
