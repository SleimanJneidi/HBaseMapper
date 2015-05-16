package com.rolonews.hbasemapper.mapping;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import com.rolonews.hbasemapper.serialisation.SerialisationManager;
import com.rolonews.hbasemapper.utils.ReflectionUtils;

/**
 * 
 * @author maamria
 *
 */
public final class FieldDescriptor {
	
	private final Field field;
	
	private final Field subField;
	
	public FieldDescriptor(Field field, Field subField){
		this.field = field;
		this.subField = subField;
	}
	
	public Field getField() {
		return field;
	}

	public Field getSubField() {
		return subField;
	}
	
	public boolean isNested(){
		return subField != null;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(this == obj){
			return true;
		}
		if(obj instanceof FieldDescriptor){
			FieldDescriptor other = (FieldDescriptor) obj;
			return Objects.equal(field, other.field) && Objects.equal(subField, other.subField);
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return Objects.hashCode(field, subField);
	}
	
	public static FieldDescriptor of(String fieldPath, Class<?> clazz, boolean isCollection){
		Preconditions.checkArgument(!Strings.isNullOrEmpty(fieldPath));
		List<String> splitPath = Lists.newArrayList(Splitter.on('.').split(fieldPath));
		int fieldPathDepth = splitPath.size();
		if(fieldPathDepth > 2){
			throw new InvalidMappingException("Mapping of fields can be done up to one level deep in the object graph");
		}
		Map<String, Field> fieldsMap = ReflectionUtils.getDeclaredAndInheritedFieldsMap(clazz);
		String fieldName = splitPath.get(0);
		Field field = fieldsMap.get(fieldName);
		if(field == null){
			throw new InvalidMappingException(String.format("Field %s does not exist for class %s", fieldName, clazz));
		}
		if(fieldPathDepth == 1){
			if(isCollection){
				checkCollectionType(field.getType());
			}
			return new FieldDescriptor(field, null);
		}
		String subFieldName = splitPath.get(1);
		Class<?> fieldType = field.getType();
		Map<String, Field> subFieldsMap = ReflectionUtils.getDeclaredAndInheritedFieldsMap(fieldType);
		Field subField = subFieldsMap.get(subFieldName);
		if(subField == null){
			throw new InvalidMappingException(String.format("Sub-field %s does not exist for class %s", subFieldName, fieldType));
		}
		if(isCollection){
			checkCollectionType(subField.getType());
		}
		return new FieldDescriptor(field, subField);
	}
	
	private static void checkCollectionType(Class<?> clazz){
		if(!List.class.isAssignableFrom(clazz) 
				&& !Map.class.isAssignableFrom(clazz) 
				&& !Set.class.isAssignableFrom(clazz)){
			throw new InvalidMappingException(clazz + " is not a collection field");
		}
	}
	
	
	
}
