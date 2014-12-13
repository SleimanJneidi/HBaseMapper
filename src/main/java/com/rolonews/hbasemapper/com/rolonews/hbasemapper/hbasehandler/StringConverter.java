package com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.NotImplementedException;

import java.math.BigDecimal;

/**
 * A utility class that converts a string to an object
 * Created by Sleiman on 13/12/2014.
 */
public class StringConverter {

    public static Object convert(Class<?> targetType,String value){
        Preconditions.checkNotNull(value);
        Preconditions.checkNotNull(targetType);
        String typeName = targetType.getName();
        if(String.class.equals(targetType)){
            return value;
        }
        if(Integer.class.equals(targetType) || "int".equals(targetType.getName())){
            return Integer.parseInt(value);
        }
        if(Double.class.equals(targetType) || "double".equals(typeName)) {
            return Double.parseDouble(value);
        }
        if(Short.class.equals(targetType) || "short".equals(typeName)){
            return Short.parseShort(value);
        }
        if(Boolean.class.equals(targetType) || "boolean".equals(typeName)){
            return Boolean.valueOf(value);
        }
        if(Long.class.equals(targetType) || "long".equals(typeName)){
            return Long.parseLong(value);
        }
        if(BigDecimal.class.equals(targetType)){
            return new BigDecimal(value);
        }
        else{
            throw new NotImplementedException("Can't convert a string of value "+value+" to an object of type "+targetType.getName());
        }

    }
}
