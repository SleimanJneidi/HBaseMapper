package com.rolonews.hbasemapper.query;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.rolonews.hbasemapper.serialisation.BasicObjectSerializer;
import com.rolonews.hbasemapper.serialisation.ObjectSerializer;
import com.rolonews.hbasemapper.SerializationFactory;
import com.rolonews.hbasemapper.mapping.CellDescriptor;
import com.rolonews.hbasemapper.mapping.EntityMapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 *
 * Created by Sleiman on 11/12/2014.
 */
public class HResultParser<T> implements ResultParser<T> {

    private final Class<T> clazz;

    private final Optional<Supplier<T>> instanceCreator;

    private final EntityMapper<T> mapper;

    public HResultParser(final Class<T> clazz,
                         final EntityMapper<T> mapper,
                         final Optional<Supplier<T>> instanceSupplier) {
        Preconditions.checkNotNull(clazz);
        Preconditions.checkNotNull(instanceSupplier);

        this.clazz = clazz;
        this.instanceCreator = instanceSupplier;
        this.mapper = mapper;
    }

    @Override
    public final T valueOf(Result result) {
        Preconditions.checkNotNull(result);
        try {
            T object;
            if(instanceCreator.isPresent()){
                object = instanceCreator.get().get();
            }else{
                Constructor<T> constructor = clazz.getDeclaredConstructor();
                constructor.setAccessible(true);
                object = constructor.newInstance();
            }

            for (CellDescriptor mColumn : mapper.columns().keySet()) {

                byte[] familyBuffer = Bytes.toBytes(mColumn.family());
                byte[] qualifierBuffer = Bytes.toBytes(mColumn.qualifier());
                byte[] resultBuffer = result.getValue(familyBuffer, qualifierBuffer);

                if (resultBuffer != null) {
                    Field field = mapper.columns().get(mColumn);
                    field.setAccessible(true);

                    ObjectSerializer serializer = SerializationFactory.getSerializer(field.getType());
                    Object desrializedBuffer = serializer.deserialize(resultBuffer, field.getType());
                    field.set(object, desrializedBuffer);
                }

            }
            return object;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }

    }

    public final <K> QueryResult<K,T> valueAsQueryResult(Class<K> keyClazz, Result result){
        Preconditions.checkNotNull(result);

        byte[] rowBuffer = result.getRow();
        ObjectSerializer serializer = new BasicObjectSerializer();
        K row = serializer.deserialize(rowBuffer, keyClazz);
        T object = valueOf(result);
        QueryResult<K,T> queryResult = new QueryResult<K, T>(row,object);

        return queryResult;
    }
}
