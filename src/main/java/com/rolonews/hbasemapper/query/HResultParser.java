package com.rolonews.hbasemapper.query;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.rolonews.hbasemapper.mapping.EntityMapper;
import com.rolonews.hbasemapper.mapping.FieldDescriptor;
import com.rolonews.hbasemapper.mapping.HCellDescriptor;
import com.rolonews.hbasemapper.serialisation.SerialisationManager;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.util.*;

/**
 *
 * Created by Sleiman on 11/12/2014.
 */
public class HResultParser<T> implements ResultParser<T> {

    private final Class<T> clazz;

    private final Optional<Supplier<T>> instanceCreator;

    private final EntityMapper<T> mapper;
    
    private final SerialisationManager serialisationManager;
    
    private Map<String, HCellDescriptor> indexedSimpleColumns;
    private Map<HCellDescriptor, FieldDescriptor> aggregateColumns;
    private Map<HCellDescriptor, FieldDescriptor> simpleColumns;
    private Map<String, HCellDescriptor> indexedAggregateColumns;

    public HResultParser(final Class<T> clazz,
                         final EntityMapper<T> mapper,
                         final Optional<Supplier<T>> instanceSupplier) {
        Preconditions.checkNotNull(clazz);
        Preconditions.checkNotNull(mapper);
        Preconditions.checkNotNull(instanceSupplier);

        this.clazz = clazz;
        this.instanceCreator = instanceSupplier;
        this.mapper = mapper;
        this.serialisationManager = this.mapper.serializationManager();
        this.partitionColumns();
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
            CellScanner cellScanner = result.cellScanner();
            
            while (cellScanner.advance()) {
				Cell currentCell = cellScanner.current();
				byte[] familyBuffer = CellUtil.cloneFamily(currentCell);
				byte[] qualifierBuffer = CellUtil.cloneQualifier(currentCell);
				byte[] resultBuffer = result.getValue(familyBuffer, qualifierBuffer);
				String qualifier = Bytes.toString(qualifierBuffer);
				String family = Bytes.toString(familyBuffer);
				String fullQualifier = dot(family, qualifier);
				if(indexedSimpleColumns.containsKey(fullQualifier)){
					HCellDescriptor cellDescriptor = indexedSimpleColumns.get(fullQualifier);
					FieldDescriptor fieldDescriptor = simpleColumns.get(cellDescriptor);
					Field field = fieldDescriptor.getField();
					Class<?> fieldType = field.getType();
					if(!fieldDescriptor.isNested()){
						field.setAccessible(true);
						Object desrializedBuffer = serialisationManager.deserialize(resultBuffer, fieldType);
						field.set(object, desrializedBuffer);
					}
					else {
						Object subObject =  field.get(object);// objectFieldValues.get(field);
						if(subObject == null){
							Constructor<?> constructor = fieldType.getDeclaredConstructor();
			                constructor.setAccessible(true);
			                subObject = constructor.newInstance();
			                field.set(object, subObject);
						}
		                Field subField = fieldDescriptor.getSubField();
		                Class<?> subFieldType = subField.getType();
		                Object desrializedBuffer = serialisationManager.deserialize(resultBuffer, subFieldType);
		                subField.set(subObject, desrializedBuffer);
					}
					
				} else {
					CollectionColumnMatch aggregateColumnMatch = new CollectionColumnMatch(fullQualifier);
					if(Maps.filterKeys(indexedAggregateColumns, aggregateColumnMatch).size() == 1){
						HCellDescriptor cellDescriptor = indexedAggregateColumns.get(aggregateColumnMatch.matchingCollectionColumn);
						FieldDescriptor fieldDescriptor = aggregateColumns.get(cellDescriptor);
						Field field = fieldDescriptor.getField();
						field.setAccessible(true);
						Class<?> fieldType = field.getType();
						if(!fieldDescriptor.isNested()){
							if(List.class.isAssignableFrom(fieldType)){
								handleList(field, object, aggregateColumnMatch, resultBuffer);
							}
							else if(Map.class.isAssignableFrom(fieldType)){
								handleMap(field, object, aggregateColumnMatch, resultBuffer);
							}
							else if(Set.class.isAssignableFrom(fieldType)){
								handleSet(field, object, aggregateColumnMatch, resultBuffer);
							}
						}
						else {
							Object subObject = field.get(object);
							if(subObject == null){
								Constructor<?> constructor = fieldType.getDeclaredConstructor();
				                constructor.setAccessible(true);
				                subObject = constructor.newInstance();
				                field.set(object, subObject);
							}
							Field subField = fieldDescriptor.getSubField();
							subField.setAccessible(true);
			                Class<?> subFieldType = subField.getType();
			                if(List.class.isAssignableFrom(subFieldType)){
			                	handleList(subField, subObject, aggregateColumnMatch, resultBuffer);
			                }
			                else if(Map.class.isAssignableFrom(subFieldType)){
			                	handleMap(subField, subObject, aggregateColumnMatch, resultBuffer);
							}
			                else if(Set.class.isAssignableFrom(subFieldType)){
			                	handleSet(subField, subObject, aggregateColumnMatch, resultBuffer);
							}
						}
					}
				}
			}
			mapper.rowKeyConsumer().consume(Pair.newPair(object, result.getRow()));
            return object;
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
        	throw new RuntimeException(e);
		}
    }

    public final <K> QueryResult<K,T> valueAsQueryResult(Class<K> keyClazz, Result result){
        Preconditions.checkNotNull(result);

        byte[] rowBuffer = result.getRow();
        K row = serialisationManager.deserialize(rowBuffer, keyClazz);
        T object = valueOf(result);

		return new QueryResult<K, T>(row,object);
    }
    
    private static final class CollectionColumnMatch implements Predicate<String>{

    	private String column;
    	private String matchingCollectionColumn;
    	
    	CollectionColumnMatch(String column){
    		this.column = column;
    	}
    	
		@Override
		public boolean apply(String columnQualifier) {
			if(column.startsWith(dot(columnQualifier, ""))){
				matchingCollectionColumn = columnQualifier;
				return true;
			}
			return false;
		}
    	
		public String getKey(){
			return column.replaceFirst(dot(matchingCollectionColumn, ""), "");
		}
		
		public int getIndex(){
			String key = getKey();
			return Integer.valueOf(key);
		}
    }
    
    private void partitionColumns(){
    	this.simpleColumns = new LinkedHashMap<HCellDescriptor, FieldDescriptor>();
    	this.aggregateColumns = new LinkedHashMap<HCellDescriptor, FieldDescriptor>();
    	this.indexedSimpleColumns = new LinkedHashMap<String, HCellDescriptor>();
    	this.indexedAggregateColumns = new LinkedHashMap<String, HCellDescriptor>();
    	Map<HCellDescriptor, FieldDescriptor> columns = mapper.columns();
		for (HCellDescriptor cellDescriptor : columns.keySet()) {
			if(cellDescriptor.isCollection()){
				this.aggregateColumns.put(cellDescriptor, columns.get(cellDescriptor));
				this.indexedAggregateColumns.put(dot(cellDescriptor.family(), cellDescriptor.qualifier()), cellDescriptor);
			}
			else {
				this.simpleColumns.put(cellDescriptor, columns.get(cellDescriptor));
				this.indexedSimpleColumns.put(dot(cellDescriptor.family(), cellDescriptor.qualifier()), cellDescriptor);
			}
		}
    }
    
    private static String dot(String str1, String str2){
    	return str1 + "." + str2;
    }
    
    private void handleList(Field field, Object parentObject, CollectionColumnMatch match, byte[] resultBuffer) throws IllegalAccessException{
    	Object object = field.get(parentObject);
    	if(object == null){
			if(LinkedList.class == field.getType()){
				object = new LinkedList();
			}
			else {
				object = new ArrayList();
			}
			field.set(parentObject, object);
		}
		List list = (List) object;
		Class<?> itemType = (Class<?>) ((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0];
		Object desrializedBuffer = serialisationManager.deserialize(resultBuffer, itemType);
		int index = 0;
		try {
			index = match.getIndex();
			list.add(index, desrializedBuffer);
		} 
		catch(NumberFormatException e){
		}
	}
    
    private void handleMap(Field field, Object parentObject, CollectionColumnMatch match, byte[] resultBuffer) throws IllegalAccessException{
    	Object object = field.get(parentObject);
    	if(object == null){
			if(SortedMap.class.isAssignableFrom(field.getType())){
				object = new TreeMap();
			}
			else {
				object = new HashMap();
			}
			field.set(parentObject, object);
		}
		Map map = (Map) object;
		Class<?> valueType = (Class<?>) ((ParameterizedType)field.getGenericType()).getActualTypeArguments()[1];
		Object desrializedValueBuffer = serialisationManager.deserialize(resultBuffer, valueType);
		String key = match.getKey();
		map.put(key, desrializedValueBuffer);
    }
    
    private void handleSet(Field field, Object parentObject, CollectionColumnMatch match, byte[] resultBuffer) throws IllegalAccessException{
    	Object object = field.get(parentObject);
    	if(object == null){
			if(SortedSet.class.isAssignableFrom(field.getType())){
				object = new TreeSet();
			}
			else {
				object = new HashSet();
			}
			field.set(parentObject, object);
		}
		Set set = (Set) object;
		Class<?> itemType = (Class<?>) ((ParameterizedType)field.getGenericType()).getActualTypeArguments()[0];
		Object desrializedBuffer = serialisationManager.deserialize(resultBuffer, itemType);
		try {
			match.getIndex();
			set.add(desrializedBuffer);
		} 
		catch(NumberFormatException e){
		}
	}
}
