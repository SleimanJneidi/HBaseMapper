package com.rolonews.hbasemapper.mapping;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.annotations.*;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import com.rolonews.hbasemapper.utils.ReflectionUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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

    private final Class<T> clazz;
    private final HTableDescriptor tableDescriptor;
    private final Function<T,?> rowKeyGenerator;
    private final Map<CellDescriptor,Field> columns;

    private AnnotationEntityMapper(Class<T> clazz, Table table, Map<CellDescriptor, Field> columns,Function<T,?> rowKeyGenerator) {
        this.clazz = clazz;
        this.columns = columns;
        this.rowKeyGenerator = rowKeyGenerator;
        this.tableDescriptor = new HTableDescriptor(TableName.valueOf(table.name()));
        for (String family : table.columnFamilies()) {
            this.tableDescriptor.addFamily(new HColumnDescriptor(family));
        }
    }

    public static <T> EntityMapper<T> createAnnotationMapping(Class<T> clazz) {

        Table tableAnnotation = getTable(clazz);

        List<Field> allFields = ReflectionUtils.getDeclaredAndInheritedFields(clazz);
        Map<CellDescriptor, Field> columns = new HashMap<CellDescriptor, Field>();

        Set<String> families = new HashSet<String>(Arrays.asList(tableAnnotation.columnFamilies()));

        for (Field field : allFields) { // validate column  familes
            Column column = field.getAnnotation(Column.class);
            if (column != null) {
                String family = column.family();
                if (!families.contains(family)) {
                    throw new InvalidMappingException(String.format("Column family %s does not exist, make sure that your class is "
                            + "annotated properly", family));
                }
                CellDescriptor cell = new HCellDescriptor(column.family(),column.qualifier());
                columns.put(cell, field);
            }

        }
        Class<? extends Function<?, ?>> keyGeneratorClazz = tableAnnotation.rowKeyGenerator();
        Function<T, ?> rowKeyGenerator;
        try {
            rowKeyGenerator = (Function<T, ?>) keyGeneratorClazz.newInstance();
        }catch (InstantiationException e){
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        EntityMapper<T> mapper = new AnnotationEntityMapper<T>(clazz, tableAnnotation, columns,rowKeyGenerator);

        return mapper;
    }

    /*
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
    */


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
    public HTableDescriptor tableDescriptor() {
        return this.tableDescriptor;
    }

    @Override
    public Function<T, ?> rowKeyGenerator() {
        return this.rowKeyGenerator;
    }

    @Override
    public Map<CellDescriptor, Field> columns() {
        return this.columns;
    }
}
