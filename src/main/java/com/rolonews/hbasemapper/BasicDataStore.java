package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.rolonews.hbasemapper.mapping.CellDescriptor;
import com.rolonews.hbasemapper.mapping.EntityMapper;
import com.rolonews.hbasemapper.mapping.HTableHandler;
import com.rolonews.hbasemapper.mapping.MappingRegistry;
import com.rolonews.hbasemapper.query.HResultParser;
import com.rolonews.hbasemapper.query.IQuery;
import com.rolonews.hbasemapper.query.QueryResult;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

import static com.rolonews.hbasemapper.serialisation.SerializationFactory.*;

/**
 *
 * Created by Sleiman on 10/12/2014.
 *
 */
public class BasicDataStore<T> implements DataStore<T> {


    private final HConnection connection;
    private final Class<T> clazz;
    private final EntityMapper<T> mapper;

    protected BasicDataStore(final HConnection connection, final Class<T> clazz, final EntityMapper<T> mapper) {
        Preconditions.checkNotNull(connection);
        Preconditions.checkNotNull(clazz);

        this.connection = connection;
        this.clazz = clazz;

        if(mapper == null){
            this.mapper = MappingRegistry.registerIfAbsent(clazz);
        }else{
            this.mapper = mapper;
        }
    }

    @Override
    public void put(final T object) {
        Preconditions.checkNotNull(object);

        byte[]rowKeyBuffer = rowKey(object);
        Put put = createPut(rowKeyBuffer,object);
        insert(Arrays.asList(put));
    }


    @Override
    public void put(List<T> objects) {
        Preconditions.checkNotNull(objects);
        if(objects.isEmpty()) return;

        List<Put> puts = new ArrayList<Put>();
        for(T object: objects){
            byte[]rowKey = rowKey(object);
            Put put = createPut(rowKey,object);
            puts.add(put);
        }
        insert(puts);
    }

    @Override
    public void put(Object key, T object) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(object);

        byte[]rowKey = getSerializer(key).serialize(key);
        Put put = createPut(rowKey,object);

        insert(Arrays.asList(put));
    }

    @Override
    public <K> void put(Function<T, K> rowKeyFunction, List<T> objects) {
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
        insert(puts);
    }

    @Override
    public <K> Optional<T> get(K key) {
        Preconditions.checkNotNull(key);
        final byte[]rowKey = getSerializer(key).serialize(key);

        HTableInterface tableInterface = null;
        try {
            HTableHandler tableHandler = new HTableHandler(this.connection);
            tableInterface = tableHandler.getOrCreateHTable(mapper);

            Get get = new Get(rowKey);

            Set<CellDescriptor> columns = mapper.columns().keySet();

            for(CellDescriptor column: columns){
                get.addColumn(Bytes.toBytes(column.family()),Bytes.toBytes(column.qualifier()));
            }

            Result result = tableInterface.get(get);
            if(result.getRow()==null){
                return Optional.absent();
            }else{
                HResultParser<T> resultParser = new HResultParser<T>(clazz,mapper,Optional.<Supplier<T>>absent());
                T object = resultParser.valueOf(result);
                return Optional.of(object);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }finally {
            if(tableInterface!=null){
                try {
                    tableInterface.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void delete(Object key) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(clazz);

        byte[]rowKey = getSerializer(key).serialize(key);
        Delete delete = new Delete(rowKey);
        deleteObjects(new ArrayList<Delete>(Arrays.asList(delete)));
    }

    @Override
    public void delete(List<?> keys) {
        Preconditions.checkNotNull(keys);
        Preconditions.checkNotNull(clazz);

        List<Delete> deletes = Lists.transform(keys, new Function<Object, Delete>() {

            @Override
            public Delete apply(Object key) {
                byte[] row = getSerializer(key).serialize(key);
                return new Delete(row);
            }
        });
        deleteObjects(deletes);

    }

    @Override
    public List<T> get(final IQuery<T> query) {
        Preconditions.checkNotNull(query);
        final Scan scan = query.getScanner();
        final List<T> results = new ArrayList<T>();

        Consumer<HTableInterface> applyScan = new Consumer<HTableInterface>() {
            @Override
            public void consume(HTableInterface hTableInterface) {
                try {
                    ResultScanner resultScanner =  hTableInterface.getScanner(scan);
                    HResultParser<T> resultParser = new HResultParser<T>(query.getType(),mapper,Optional.<Supplier<T>>absent());
                    for (Result result : resultScanner) {
                        T object = resultParser.valueOf(result);
                        results.add(object);
                    }
                    resultScanner.close();

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        operateOnTable(applyScan);

        return results;
    }

    @Override
    public <K> List<QueryResult<K, T>> getAsQueryResult(final Class<K> rowKeyClazz,final IQuery<T> query) {

        Preconditions.checkNotNull(query);
        final Scan scan = query.getScanner();
        final List<QueryResult<K,T>> results = new ArrayList<QueryResult<K, T>>();

        Consumer<HTableInterface> applyScan = new Consumer<HTableInterface>() {
            @Override
            public void consume(HTableInterface hTableInterface) {
                try {
                    ResultScanner resultScanner =  hTableInterface.getScanner(scan);
                    HResultParser<T> resultParser = new HResultParser<T>(query.getType(),mapper,Optional.<Supplier<T>>absent());
                    for (Result result : resultScanner) {
                        QueryResult<K, T> queryResult = resultParser.valueAsQueryResult(rowKeyClazz, result);
                        results.add(queryResult);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

        operateOnTable(applyScan);

        return results;
    }


    private byte[]rowKey(final T object){

        byte[]rowKeyBuffer;
        Function<T, ?> rowKeyGenerator =  mapper.rowKeyGenerator();
        Object rowKey = rowKeyGenerator.apply(object);

        rowKeyBuffer = getSerializer(rowKey).serialize(rowKey);
        return rowKeyBuffer;
    }

    private Put createPut(final byte[] rowKey,final Object object){
        try {
            Put put = new Put(rowKey);
            Map<CellDescriptor, Field> columns = mapper.columns();
            for (Map.Entry<CellDescriptor, Field> columnFieldEntry : columns.entrySet()) {
                Field field = columnFieldEntry.getValue();
                CellDescriptor column = columnFieldEntry.getKey();
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

    private void deleteObjects(final List<Delete> deletes){

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
        operateOnTable(deleteOperations);
    }

    private void insert(final List<Put> puts){

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
        operateOnTable(putOperations);
    }

    private void operateOnTable(Consumer<HTableInterface> tableInterfaceConsumer){
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
