package com.rolonews.hbasemapper;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.rolonews.hbasemapper.annotations.Column;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.regex.Pattern;

/**
 *
 * Created by Sleiman on 11/12/2014.
 */
public class HResultParser<T> implements ResultParser<T> {

    private final Class<T> clazz;

    private final Optional<Supplier<T>> instanceCreator;


    public HResultParser(final Class<T> clazz,final Optional<Supplier<T>> instanceSupplier) {
        Preconditions.checkNotNull(clazz);
        Preconditions.checkNotNull(instanceSupplier);

        this.clazz = clazz;
        this.instanceCreator = instanceSupplier;
    }

    @Override
    public T valueOf(Result result) {
        Preconditions.checkNotNull(result);

        EntityMapper<?> typeInfo = AnnotationEntityMapper.getOrRegisterAnnotationEntityMapper(clazz);

        try {
            T object = instanceCreator.isPresent() ? instanceCreator.get().get() : clazz.newInstance();

            ObjectSerializer serializer = new BasicObjectSerializer();

            for (Column mColumn : typeInfo.columns().keySet()) {

                byte[] familyBuffer = Bytes.toBytes(mColumn.family());
                byte[] qualifierBuffer = Bytes.toBytes(mColumn.qualifier());
                byte[] resultBuffer = result.getValue(familyBuffer, qualifierBuffer);

                if (resultBuffer != null) {
                    Field field = typeInfo.columns().get(mColumn);
                    field.setAccessible(true);
                    Object desrializedBuffer = serializer.deserialize(resultBuffer, field.getType());
                    field.set(object, desrializedBuffer);
                }

            }
            byte[]resultRowKey = result.getRow();
            Map<String,Field> rowKeyMap = typeInfo.rowKeys();

            if(rowKeyMap.size() == 1){
                Field rowField =  (Field)rowKeyMap.values().toArray()[0];
                rowField.setAccessible(true);
                Object rowObject = serializer.deserialize(resultRowKey,rowField.getType());
                rowField.set(object,rowObject);
            }else{
                String rowAsString = Bytes.toString(resultRowKey);
                String[] rowkeyStructure = typeInfo.table().rowKey();
                String []rowSplit = rowAsString.split(Pattern.quote(typeInfo.table().rowKeySeparator()));

                for (int i=0;i<rowkeyStructure.length;i++) {

                    Field keyPartField = rowKeyMap.get(rowkeyStructure[i]);
                    keyPartField.setAccessible(true);
                    String split = rowSplit[i];

                    Object keyPartAsObject = StringConverter.convert(keyPartField.getType(),split);
                    keyPartField.set(object,keyPartAsObject);
                }

            }
            return object;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }

}
