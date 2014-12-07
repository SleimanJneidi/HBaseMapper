package com.rolonews.hbasemapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.rolonews.hbasemapper.annotations.*;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import org.slf4j.*;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * Created by Sleiman on 06/12/2014.
 *
 */
public final class HTypeInfo {

    private static final Logger LOG = LoggerFactory.getLogger(HTypeInfo.class);
    private static final Map<Class<?>, HTypeInfo> typeMap = new ConcurrentHashMap<Class<?>, HTypeInfo>();

    private final Table table;
    private final Class<?> clazz;
    private final Map<String, Field> rowKeys;
    private final Map<Column, Field> columns;
    private final List<HValidate> validators;

    private HTypeInfo(Class<?> clazz, Table table, Map<Column, Field> columns, Map<String, Field> rowKeys, List<HValidate> validators) {
        this.clazz = clazz;
        this.table = table;
        this.columns = columns;
        this.rowKeys = rowKeys;
        this.validators = validators;
    }

    public static HTypeInfo register(Class<?> clazz) {
        Preconditions.checkNotNull(clazz);

        if (typeMap.containsKey(clazz)) {
            LOG.warn(clazz.getName() + " is already mapped");
            return typeMap.get(clazz);
        }

        HTypeInfo hTypeInfo = createHTypeInfo(clazz);
        typeMap.put(clazz, hTypeInfo);
        return hTypeInfo;
    }

    private static HTypeInfo createHTypeInfo(Class<?> clazz) {

        Table tableAnnotation = clazz.getAnnotation(Table.class);

        if (tableAnnotation == null) {
            throw new InvalidMappingException(clazz.getName() + " is not annotated by " + Table.class.getName());
        }

        String[] rowKeys = tableAnnotation.rowKeys();

        List<Field> allFields = getDeclaredAndInheritedFields(clazz);
        Map<Column, Field> columns = new HashMap<Column, Field>();
        Map<String, Field> rowKeysFields = new HashMap<String, Field>();

        Map<String, Field> map = Maps.uniqueIndex(allFields, new Function<Field, String>() {
            @Override
            public String apply(Field field) {
                return field.getName();
            }

        });

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
        HTypeInfo hTypeInfo = new HTypeInfo(clazz, tableAnnotation, columns, rowKeysFields, validators);
        return hTypeInfo;
    }

    private static List<Field> getDeclaredAndInheritedFields(final Class<?> clazz) {
        final List<Field> allFields = new ArrayList<Field>();
        allFields.addAll(Arrays.asList(clazz.getDeclaredFields()));

        Class<?> parent = clazz.getSuperclass();
        while ((parent != null) && (parent != Object.class)) {
            allFields.addAll(Arrays.asList(parent.getDeclaredFields()));
            parent = parent.getSuperclass();
        }

        return allFields;
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

    /**
     * @param clazz
     * @return the HTypeInfo
     */
    public static HTypeInfo getHTypeInfo(Class<?> clazz) {
        return typeMap.get(clazz);
    }

    public static HTypeInfo getOrRegisterHTypeInfo(Class<?> clazz) {
        if (!typeMap.containsKey(clazz)) {
            register(clazz);
        }
        return typeMap.get(clazz);
    }

    public Table getTable() {
        return this.table;
    }

    public Class<?> getClazz() {
        return this.clazz;
    }

    public Map<String, Field> getRowKeys() {
        return this.rowKeys;
    }

    public Map<Column, Field> getColumns() {
        return this.columns;
    }

    public List<HValidate> getValidators(){
        return this.validators;
    }
}
