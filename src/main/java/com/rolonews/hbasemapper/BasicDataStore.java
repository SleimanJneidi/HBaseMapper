package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import com.rolonews.hbasemapper.exceptions.ValidationException;
import com.rolonews.hbasemapper.mapping.*;
import com.rolonews.hbasemapper.query.HResultParser;
import com.rolonews.hbasemapper.query.IQuery;
import com.rolonews.hbasemapper.query.QueryResult;
import com.rolonews.hbasemapper.serialisation.SerialisationManager;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 *
 * Created by Sleiman on 10/12/2014.
 *
 */
public class BasicDataStore<T> implements DataStore<T> {


    private final HConnection connection;
    private final Class<T> clazz;
    private final EntityMapper<T> mapper;
    private final SerialisationManager serializationManager;
    
    private final HObjectMapper<T> objectMapper;
    final HResultParser<T> resultParser;

    protected BasicDataStore(final HConnection connection, final Class<T> clazz, final EntityMapper<T> mapper) {
        Preconditions.checkNotNull(connection);
        Preconditions.checkNotNull(clazz);

        this.connection = connection;
        this.clazz = clazz;

        if(mapper == null){
            this.mapper = MappingRegistry.getMapping(clazz);
        }else{
            this.mapper = mapper;
        }
        if(this.mapper == null){
        	throw new InvalidMappingException("No mapping provided for class: "+ clazz);
        }
        this.serializationManager = this.mapper.serializationManager();
        this.objectMapper = new HObjectMapper<T>(this.mapper);
        this.resultParser = new HResultParser<T>(clazz, this.mapper, Optional.<Supplier<T>>absent());
    }

    @Override
    public Put put(final T object) {
        Preconditions.checkNotNull(object);
        validateObjects(Arrays.asList(object));
        byte[]rowKeyBuffer = rowKey(object);
        Put put = objectMapper.getPut(rowKeyBuffer,object);
        insert(Arrays.asList(put));
        return put;
    }


    @Override
    public List<Put> put(List<T> objects) {
        Preconditions.checkNotNull(objects);
        if(objects.isEmpty()) return null;
        validateObjects(objects);

        List<Put> puts = new ArrayList<Put>();
        for(T object: objects){
            byte[]rowKey = rowKey(object);
            Put put = objectMapper.getPut(rowKey,object);
            puts.add(put);
        }
        insert(puts);
        return puts;
    }

    @Override
    public Put put(Object key, T object) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(object);
        validateObjects(Arrays.asList(object));

        byte[]rowKey = serializationManager.serialize(key);
        Put put = objectMapper.getPut(rowKey,object);

        insert(Arrays.asList(put));
        return put;
    }

    @Override
    public <K> Optional<T> get(K key) {
        Preconditions.checkNotNull(key);
        final byte[]rowKey = serializationManager.serialize(key);

        HTableInterface tableInterface = null;
        try {
            HTableHandler tableHandler = new HTableHandler(this.connection);
            tableInterface = tableHandler.getOrCreateHTable(mapper);
            Get get = new Get(rowKey);
            Filter filter = getGetFilter();
			get.setFilter(filter);	
            Result result = tableInterface.get(get);
            if(result.getRow() == null){
                return Optional.absent();
            } else{
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
    public Delete delete(Object key) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(clazz);

        byte[]rowKey = serializationManager.serialize(key);
        Delete delete = new Delete(rowKey);
        deleteObjects(new ArrayList<Delete>(Arrays.asList(delete)));
        return delete;
    }

    @Override
    public List<Delete> delete(List<?> keys) {
        Preconditions.checkNotNull(keys);
        Preconditions.checkNotNull(clazz);

        List<Delete> deletes = new ArrayList<Delete>(keys.size());
        for (Object key : keys) {
            byte[] row = serializationManager.serialize(key);
            Delete delete = new Delete(row);
            deletes.add(delete);
        }
        deleteObjects(deletes);
        return deletes;
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

    private Filter getGetFilter(){
    	Set<HCellDescriptor> columns = mapper.columns().keySet();
        List<Filter> filters = new ArrayList<Filter>();
        for(HCellDescriptor column: columns){
        	if(column.isCollection()){
        		String prefix = collectionItemQualifier(column.qualifier(), "");
				byte[] qualifierPrefixBytes = Bytes.toBytes(prefix);
				QualifierFilter filter = new QualifierFilter(CompareOp.EQUAL, new BinaryPrefixComparator(qualifierPrefixBytes));
        		filters.add(filter);
        	}
        	else {
        		QualifierFilter qualifierFilter = new QualifierFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(column.qualifier())));
        		filters.add(qualifierFilter);
        	}
        }
        FilterList filterList = new FilterList(Operator.MUST_PASS_ONE, filters);
        return filterList;
    }

    private byte[]rowKey(final T object){

        byte[]rowKeyBuffer;
        Function<T, ?> rowKeyGenerator =  mapper.rowKeyGenerator();
        Object rowKey = rowKeyGenerator.apply(object);

        rowKeyBuffer = serializationManager.serialize(rowKey);
        return rowKeyBuffer;
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

    void operateOnTable(Consumer<HTableInterface> tableInterfaceConsumer){
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
    
    private String collectionItemQualifier(String prefix, Object keyOrIndex){
    	return prefix + "." + keyOrIndex;
    }

    private void validateObjects(List<T> objects){
        for (T object : objects) {
            if(!this.mapper.validator().apply(object)){
                throw new ValidationException(this.mapper.validator());
            }
        }
    }

}
