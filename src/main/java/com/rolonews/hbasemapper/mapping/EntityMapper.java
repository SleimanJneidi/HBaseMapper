package com.rolonews.hbasemapper.mapping;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.rolonews.hbasemapper.Consumer;
import com.rolonews.hbasemapper.serialisation.SerialisationManager;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Map;

/**
 * Created by Sleiman on 25/01/2015.
 */
public interface EntityMapper<T>{
    Class<T> clazz();
    HTableDescriptor tableDescriptor();
    Function<T,?> rowKeyGenerator();
    Consumer<Pair<T, byte[]>> rowKeyConsumer();
    Map<HCellDescriptor,FieldDescriptor> columns();
    SerialisationManager serializationManager();
    Predicate<T> validator();
    EntityMapper<T> register();
}