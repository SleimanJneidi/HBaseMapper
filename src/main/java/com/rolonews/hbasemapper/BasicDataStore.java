package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.rolonews.hbasemapper.annotations.MColumn;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.BasicObjectSerializer;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.HTableHandler;
import com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.ObjectSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

/**
 *
 * Created by Sleiman on 10/12/2014.
 *
 */
public class BasicDataStore extends DataStore {

    private final ObjectSerializer serializer = new BasicObjectSerializer();


    protected BasicDataStore(final HConnection connection) {
        super(connection);
    }

    @Override
    public void put(final Object object) {
        Preconditions.checkNotNull(object);

        byte[]rowKeyBuffer = rowKey(object);
        Put put = createPut(rowKeyBuffer,object);
        insert(Arrays.asList(put),object.getClass());
    }

    @Override
    public <T> void put(List<T> objects, Class<T> clazz) {
        Preconditions.checkNotNull(objects);
        Preconditions.checkArgument(objects.size()>=1);
        List<Put> puts = new ArrayList<Put>();
        for(Object object: objects){
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

        byte[]rowkey = serializer.serialize(key);
        Put put = createPut(rowkey,object);

    }

    @Override
    public <K, V> List<V> get(K key) {
        return null;
    }

    @Override
    public void delete(Object object) {

    }

    @Override
    public void delete(Object key, Class<?> clazz) {

    }

    private byte[]rowKey(final Object object){
        byte[]rowKeyBuffer;
        try {
            HTypeInfo typeInfo = HTypeInfo.getHTypeInfo(object.getClass());
            if (typeInfo.getRowKeys().size() == 1) {
                Field rowField = Iterables.getLast(typeInfo.getRowKeys().values());
                rowField.setAccessible(true);
                rowKeyBuffer = serializer.serialize(rowField.get(object));
            } else {
                Collection<Object> rowkeyObjects = Collections2.transform(typeInfo.getRowKeys().values(), new Function<Field, Object>() {
                    @Nullable
                    @Override
                    public Object apply(Field field) {
                        field.setAccessible(true);
                        try {
                            return field.get(object);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                String rowKeyStringValue = StringUtils.join(rowkeyObjects, typeInfo.getTable().rowKeySeperator());
                rowKeyBuffer = serializer.serialize(rowKeyStringValue);
            }
        }catch (IllegalAccessException e){
            throw new RuntimeException(e);
        }
        return rowKeyBuffer;
    }

    private Put createPut(final byte[] rowKey,final Object object){
        try {
            Put put = new Put(rowKey);
            Map<MColumn, Field> columns = HTypeInfo.getHTypeInfo(object.getClass()).getColumns();
            for (Map.Entry<MColumn, Field> columnFieldEntry : columns.entrySet()) {
                Field field = columnFieldEntry.getValue();
                MColumn column = columnFieldEntry.getKey();
                field.setAccessible(true);
                Object fieldValue = field.get(object);
                if (fieldValue != null) {
                    byte[] buffer = serializer.serialize(fieldValue);
                    put.add(Bytes.toBytes(column.family()), Bytes.toBytes(column.qualifier()), buffer);
                }

            }
            return put;
        }catch (IllegalAccessException e){
            throw new RuntimeException(e);
        }

    }

    private void insert(final List<Put> put,Class<?> clazz){
        HTypeInfo typeInfo = HTypeInfo.getHTypeInfo(clazz);
        HTableInterface table = null;
        try {

            table = new HTableHandler(connection).getOrCreateHTable(typeInfo);
            table.put(put);

        }catch (IOException e){
            throw new RuntimeException(e);
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
