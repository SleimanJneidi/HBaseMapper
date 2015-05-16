package com.rolonews.hbasemapper.mapping;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.serialisation.SerialisationManager;

/**
 * 
 * @author maamria
 *
 * @param <T>
 */
public class HObjectMapper<T> {
	
	private final EntityMapper<T> mapper;
	
	private final SerialisationManager serialisationManager;
	
	public HObjectMapper(final EntityMapper<T> mapper){
        Preconditions.checkNotNull(mapper);
        this.mapper = mapper;
        this.serialisationManager = mapper.serializationManager();
	}
	
	public Put getPut(final byte[] rowKey,final T object){
		Preconditions.checkNotNull(rowKey);
		Preconditions.checkNotNull(object);
		try {
            Put put = new Put(rowKey);
            Map<HCellDescriptor, FieldDescriptor> columns = mapper.columns();
            for (Map.Entry<HCellDescriptor, FieldDescriptor> columnFieldEntry : columns.entrySet()) {
                FieldDescriptor fieldDescriptor = columnFieldEntry.getValue();
                HCellDescriptor column = columnFieldEntry.getKey();
                Field  field = fieldDescriptor.getField();
                field.setAccessible(true);
                Object fieldValue = field.get(object);
                Class<?> fieldType = field.getType();
                if(fieldValue == null){
                	continue;
                }
                if(fieldDescriptor.isNested()){
                    Field subField = fieldDescriptor.getSubField();
                    subField.setAccessible(true);
                    fieldValue = subField.get(fieldValue);
                    fieldType = subField.getType();
                }
                field.setAccessible(true);
                if(column.isCollection()){

                	if(List.class.isAssignableFrom(fieldType)){
                		List<?> listValue = (List<?>) fieldValue;
                		addToPut(put, listValue, column);
                	}
                	else if(Map.class.isAssignableFrom(fieldType)){
                		Map<?, ?> mapValue = (Map<?, ?>) fieldValue;
                		addToPut(put, mapValue, column);
                	}
                	else if(Set.class.isAssignableFrom(fieldType)){
                		Set<?> setValue = (Set<?>) fieldValue;
                		addToPut(put, setValue, column);
                	}
                }
                else {
                	if (fieldValue != null) {
                        byte[] buffer = serialisationManager.serialize(fieldValue);
                        put.add(Bytes.toBytes(column.family()), Bytes.toBytes(column.qualifier()), buffer);
                    }
                }
            }
            return put;
        }catch (IllegalAccessException e){
            throw new RuntimeException(e);
        }
	}
	
	private Put addToPut(Put put, List<?> listValue, HCellDescriptor column){
		for (int i = 0; i < listValue.size(); i++) {
			byte[] buffer = serialisationManager.serialize(listValue.get(i));
            put.add(Bytes.toBytes(column.family()), Bytes.toBytes(collectionItemQualifier(column.qualifier(), i)), buffer);
		}
        return put;
    }
    
    private Put addToPut(Put put, Set<?> setValue, HCellDescriptor column){
    	int i = 0;
    	for (Object object : setValue) {
    		byte[] buffer = serialisationManager.serialize(object);
            put.add(Bytes.toBytes(column.family()), Bytes.toBytes(collectionItemQualifier(column.qualifier(), i)), buffer);
            i++;
		}
        return put;
    }
    
    private Put addToPut(Put put, Map<?, ?> mapValue, HCellDescriptor column){
		for (Object key : mapValue.keySet()) {
			byte[] buffer = serialisationManager.serialize(mapValue.get(key));
            put.add(Bytes.toBytes(column.family()), Bytes.toBytes(collectionItemQualifier(column.qualifier(), key)), buffer);
		}
        return put;
    }
    
    private String collectionItemQualifier(String prefix, Object keyOrIndex){
        Preconditions.checkNotNull(keyOrIndex);
    	return prefix + "." + keyOrIndex;
    }
	
}
