package com.rolonews.hbasemapper.mapping;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.HTableDescriptor;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Created by Sleiman on 25/01/2015.
 */
public interface EntityMapper<T>{
    Class<T> clazz();
    HTableDescriptor tableDescriptor();
    Function<T,?> rowKeyGenerator();
    Map<CellDescriptor,Field> columns();
    EntityMapper<T> register();
}