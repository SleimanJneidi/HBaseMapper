package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.rolonews.hbasemapper.annotations.Column;
import com.rolonews.hbasemapper.query.IQuery;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import static com.rolonews.hbasemapper.SerializationFactory.*;

/**
 *
 * Created by Sleiman on 10/12/2014.
 *
 */
public class BasicDataStore implements DataStore {


    private final HConnection connection;

    protected BasicDataStore(final HConnection connection) {
        this.connection = connection;
    }

    @Override
    public<T> void put(final T object) {
        Preconditions.checkNotNull(object);

        byte[]rowKeyBuffer = rowKey(object);
        Put put = createPut(rowKeyBuffer,object);
        insert(Arrays.asList(put),object.getClass());
    }


    @Override
    public <T> void put(List<T> objects, Class<T> clazz) {
        Preconditions.checkNotNull(objects);
        Preconditions.checkArgument(objects.size()>=1);
        Preconditions.checkNotNull(clazz);

        List<Put> puts = new ArrayList<Put>();
        for(T object: objects){
            byte[]rowKey = rowKey(object);
            Put put = createPut(rowKey,object);
            puts.add(put);
        }
        insert(puts, clazz);
    }

    @Override
    public void put(Object key, Object object) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(object);

        byte[]rowKey = getSerializer(key).serialize(key);
        Put put = createPut(rowKey,object);

        insert(Arrays.asList(put),object.getClass());
    }

    @Override
    public <K, T> void put(Function<T, K> rowKeyFunction, List<T> objects, Class<T> clazz) {
        Preconditions.checkNotNull(objects);
        Preconditions.checkArgument(objects.size()>=1);

        Preconditions.checkNotNull(rowKeyFunction);
        Preconditions.checkNotNull(clazz);


        List<Put> puts = new ArrayList<Put>();
        for(T object: objects){
            Object rowKeyValue = rowKeyFunction.apply(object);
            byte[]rowKey = getSerializer(rowKeyValue).serialize(rowKeyValue);
            Put put = createPut(rowKey,object);
            puts.add(put);
        }
        insert(puts, clazz);
    }

    @Override
    public <K, T> Optional<T> get(K key, Class<T> clazz) {
        Preconditions.checkNotNull(key);
        EntityMapper<?> mapper = MappingRegistry.registerIfAbsent(clazz);
        byte[]rowKey = getSerializer(key).serialize(key);
        try {

            HTableInterface tableInterface =  connection.getTable(mapper.tableDescriptor().getNameAsString());

            Get get = new Get(rowKey);

            Set<Column> columns = mapper.columns().keySet();

            for(Column column: columns){
                get.addColumn(Bytes.toBytes(column.family()),Bytes.toBytes(column.qualifier()));
            }

            Result result = tableInterface.get(get);
            if(result.getRow()==null){
                return Optional.absent();
            }else{
                HResultParser<T> resultParser = new HResultParser<T>(clazz,Optional.<Supplier<T>>absent());
                T object = resultParser.valueOf(result);
                return Optional.of(object);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(Object key, Class<?> clazz) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(clazz);

        byte[]rowKey = getSerializer(key).serialize(key);
        Delete delete = new Delete(rowKey);
        deleteObjects(new ArrayList<Delete>(Arrays.asList(delete)), clazz);
    }

    @Override
    public void delete(List<?> keys, Class<?> clazz) {
        Preconditions.checkNotNull(keys);
        Preconditions.checkNotNull(clazz);

        List<Delete> deletes = Lists.transform(keys, new Function<Object, Delete>() {

            @Nullable
            @Override
            public Delete apply(@Nullable Object key) {
                byte[] row = getSerializer(key).serialize(key);
                return new Delete(row);
            }
        });
        deleteObjects(deletes,clazz);

    }

    @Override
    public <T> List<T> get(final IQuery<T> queryBuilder) {
        Preconditions.checkNotNull(queryBuilder);
        final Scan scan = queryBuilder.getScanner();
        final List<T> results = new ArrayList<T>();

        Consumer<HTableInterface> applyScan = new Consumer<HTableInterface>() {
            @Override
            public void consume(HTableInterface hTableInterface) {
                try {
                    ResultScanner resultScanner =  hTableInterface.getScanner(scan);
                    HResultParser<T> resultParser = new HResultParser<T>(queryBuilder.getType(),Optional.<Supplier<T>>absent());
                    for (Result result : resultScanner) {
                        T object = resultParser.valueOf(result);
                        results.add(object);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        operateOnTable(applyScan,queryBuilder.getType());

        return results;
    }

    private <T> byte[]rowKey(final T object){
        byte[]rowKeyBuffer;

        EntityMapper<T> mapper = MappingRegistry.registerIfAbsent((Class<T>) object.getClass());
        Function<T, ?> rowKeyGenerator =  mapper.rowKeyGenerator();
        Object rowKey = rowKeyGenerator.apply(object);

        rowKeyBuffer = getSerializer(rowKey).serialize(rowKey);
        return rowKeyBuffer;
    }

    private Put createPut(final byte[] rowKey,final Object object){
        try {
            Put put = new Put(rowKey);
            Map<Column, Field> columns = MappingRegistry.registerIfAbsent(object.getClass()).columns();
            for (Map.Entry<Column, Field> columnFieldEntry : columns.entrySet()) {
                Field field = columnFieldEntry.getValue();
                Column column = columnFieldEntry.getKey();
                field.setAccessible(true);
                Object fieldValue = field.get(object);
                if (fieldValue != null) {
                    byte[] buffer = getSerializer(fieldValue).serialize(fieldValue);
                    put.add(Bytes.toBytes(column.family()), Bytes.toBytes(column.qualifier()), buffer);
                }

            }
            return put;
        }catch (IllegalAccessException e){
            throw new RuntimeException(e);
        }

    }

    private void deleteObjects(final List<Delete> deletes,Class<?> clazz){

        Consumer<HTableInterface> deleteOperations = new Consumer<HTableInterface>() {
            @Override
            public void consume(HTableInterface hTableInterface) {
                try {
                    hTableInterface.delete(deletes);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        operateOnTable(deleteOperations,clazz);
    }

    private void insert(final List<Put> puts,Class<?> clazz){

            Consumer<HTableInterface> putOperations = new Consumer<HTableInterface>() {
                @Override
                public void consume(HTableInterface hTableInterface) {
                    try {
                        hTableInterface.put(puts);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        operateOnTable(putOperations,clazz);
    }

    private void operateOnTable(Consumer<HTableInterface> tableInterfaceConsumer, Class<?> clazz){
        EntityMapper<?> mapper = MappingRegistry.registerIfAbsent(clazz);
        HTableInterface table = null;
        try {
            table = new HTableHandler(connection).getOrCreateHTable(mapper);
            tableInterfaceConsumer.consume(table);
        }finally {
            if(table != null){
                try {
                    table.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
