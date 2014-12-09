package com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler;

import com.google.common.base.Preconditions;
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

        if ("java.lang.String".equals(typeName)) {
            return Bytes.toBytes((String) object);
        }
        if("java.lang.Integer".equals(typeName) || "int".equals(typeName)){
            return Bytes.toBytes((Integer)object);
        }
        if ("java.lang.Double".equals(typeName) || "double".equals(typeName)) {
            return Bytes.toBytes((Double)object);
        }
        if("java.lang.short".equals(typeName) || "short".equals(typeName)){
            return Bytes.toBytes((Short)object);
        }
        if("java.lang.boolean".equals(typeName) || "boolean".equals(typeName)){
            return Bytes.toBytes((Boolean)object);
        }
        if("java.lang.Long".equals(typeName) || "long".equals(typeName)){
            return Bytes.toBytes((Long)object);
        }
        if("java.math.BigDecimal".equals(typeName)){
            return Bytes.toBytes((BigDecimal)object);
        }
        if("java.nio.ByteBuffer".equals(typeName)){
            return Bytes.toBytes((ByteBuffer)object);
        }
        else{
            throw new NotImplementedException("Can't serialize object of type "+typeName);
        }
    }

    @Override
    public Object deserialize(byte[] buffer) {
        return null;
    }
}
