package com.rolonews.hbasemapper;
import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.annotations.*;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import com.rolonews.hbasemapper.utils.ReflectionUtils;
import org.slf4j.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Created by Sleiman on 06/12/2014.
 *
 */
public final class AnnotationEntityMapper<T> implements EntityMapper<T> {

    private final Table table;
    private final Class<T> clazz;
    private final Map<String, Field> rowKeys;
    private final Map<Column, Field> columns;
    private final List<HValidate> validators;

    private AnnotationEntityMapper(Class<T> clazz, Table table, Map<Column, Field> columns, Map<String, Field> rowKeys, List<HValidate> validators) {
        this.clazz = clazz;
        this.table = table;
        this.columns = columns;
        this.rowKeys = rowKeys;
        this.validators = validators;
    }

    public static <T> EntityMapper<T> register(Class<T> clazz) {
        Preconditions.checkNotNull(clazz);

        EntityMapper<T> mapper = createAnnotationMapping(clazz);
        MappingRegistry.register(mapper);
        return mapper;
    }

    private static <T> EntityMapper<T> createAnnotationMapping(Class<T> clazz) {

        Table tableAnnotation = getTable(clazz);

        String[] rowKeys = tableAnnotation.rowKey();

        List<Field> allFields = ReflectionUtils.getDeclaredAndInheritedFields(clazz);
        Map<Column, Field> columns = new HashMap<Column, Field>();
        Map<String, Field> rowKeysFields = new HashMap<String, Field>();

        Map<String, Field> map = ReflectionUtils.getDeclaredAndInheritedFieldsMap(clazz);

        for (String rowKey : rowKeys) { // check if there are fields that match row keys
            if (!map.containsKey(rowKey)) {
                throw new InvalidMappingException(rowKey + " is not a field");
            } else {
                rowKeysFields.put(rowKey, map.get(rowKey));
            }
        }

        Set<String> families = new HashSet<String>(Arrays.asList(tableAnnotation.columnFamilies()));

        for (Field field : allFields) { // validate column  familes
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                String family = column.family();
                if (!families.contains(family)) {
                    throw new InvalidMappingException(String.format("Column family %s does not exist, make sure that your class is "
                            + "annotated properly", family));
                }
                columns.put(column, field);
            }

        }

        // get validator
        List<HValidate> validators = getDeclaredAndInheritedValidators(clazz);
        EntityMapper<T> mapper = new AnnotationEntityMapper<T>(clazz, tableAnnotation, columns, rowKeysFields, validators);
        return mapper;
    }


    private static List<HValidate> getDeclaredAndInheritedValidators(final Class<?> clazz){
        final List<HValidate> validators = new ArrayList<HValidate>();
        HValidate hValidate =  clazz.getAnnotation(HValidate.class);

        if(hValidate!=null){
            validators.add(hValidate);
        }
        Class<?> parent = clazz.getSuperclass();
        while((parent!=null) && (parent != Object.class)){
            HValidate parentHValidate = parent.getAnnotation(HValidate.class);
            if(parentHValidate != null){
                validators.add(parentHValidate);
            }
            parent = parent.getSuperclass();
        }
        return validators;
    }



    public static <T> EntityMapper<T> getOrRegisterAnnotationEntityMapper(Class<T> clazz) {
        if (MappingRegistry.getMapping(clazz)==null) {
            register(clazz);
        }
        return MappingRegistry.getMapping(clazz);
    }


    public List<HValidate> getValidators(){
        return this.validators;
    }

    private static Table getTable(Class<?> clazz){
        Table tableAnnotation = clazz.getAnnotation(Table.class);
        if(tableAnnotation != null){
            return tableAnnotation;
        }
        Class<?> parent = clazz.getSuperclass();
        while (parent !=null && parent!= Object.class){
            if(parent.getAnnotation(Table.class)!=null){
                return parent.getAnnotation(Table.class);
            }
            parent = parent.getSuperclass();
        }
        throw new InvalidMappingException(clazz.getName() + " is not annotated by " + Table.class.getName());

    }

    @Override
    public Class<T> clazz() {
        return this.clazz;
    }

    @Override
    public Table table() {
        return this.table;
    }

    @Override
    public Map<String, Field> rowKeys() {
        return this.rowKeys;
    }

    @Override
    public Map<Column, Field> columns() {
        return this.columns;
    }
}
