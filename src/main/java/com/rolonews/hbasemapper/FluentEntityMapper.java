package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.annotations.Column;
import com.rolonews.hbasemapper.annotations.Table;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import com.rolonews.hbasemapper.utils.ReflectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Triple;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;

/**
 *
 * Created by Sleiman on 25/01/2015.
 */

class FluentEntityMapper<T> implements EntityMapper<T> {

    private final Class<T> clazz;
    private final Map<CellDescriptor, Field> columns;
    private final HTableDescriptor tableDescriptor;
    private final Function<T,?> rowKeyGenerator;

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

    private FluentEntityMapper(Builder<T> builder){
        this.clazz = builder.clazz;
        this.columns = builder.columns;
        this.tableDescriptor = new HTableDescriptor(TableName.valueOf(builder.tableName));

        for (String family : builder.columnFamilies) {
            this.tableDescriptor.addFamily(new HColumnDescriptor(family));
        }
        this.rowKeyGenerator = builder.rowKeyGenerator;

    }

    public static <T> Builder<T> builder(Class<T> clazz, String tableName){
        Builder<T> mapperBuilder = new Builder<T>(clazz,tableName);
        return mapperBuilder;
    }

    public static class Builder<T>{

        private String tableName;
        private Class<T> clazz;
        private final Set<String> columnFamilies = new HashSet<String>();
        private Function<T,?> rowKeyGenerator;

        private List<Triple<String,String,String>> columnsFields = new ArrayList<Triple<String, String,String>>();

        private Map<CellDescriptor, Field> columns;

        public Builder<T> withTable(String tableName){
            this.tableName = tableName;
            return this;
        }

        public Builder<T> withRowKeyGenerator(Function<T,?> rowKeyGenerator){
            this.rowKeyGenerator = rowKeyGenerator;
            return this;
        }

        public Builder<T> withColumnQualifier(String family,String qualifier, String fieldName){
            columnFamilies.add(family);
            this.columnsFields.add(new Triple<String, String,String>(family,qualifier,fieldName));
            return this;
        }

        public Builder(Class<T> clazz, String tableName){
            Preconditions.checkNotNull(clazz);
            Preconditions.checkNotNull(tableName);
            this.clazz = clazz;
            this.tableName = tableName;
        }

        public FluentEntityMapper<T> build(){
            Preconditions.checkArgument(StringUtils.isNotBlank(tableName));
            Preconditions.checkNotNull(rowKeyGenerator);
            Preconditions.checkArgument(columnFamilies.size()>=1);

            Map<String, Field> map = ReflectionUtils.getDeclaredAndInheritedFieldsMap(clazz);

            columns = new HashMap<CellDescriptor, Field>();

            for (Triple<String, String, String> columnsField : columnsFields) {
                final String family = columnsField.getFirst();
                final String qualifier = columnsField.getSecond();
                final String field = columnsField.getThird();


                if(!map.containsKey(field)){
                    throw new InvalidMappingException(field + " is not a field");
                }else{
                    columns.put(new HCellDescriptor(family,qualifier),map.get(field));
                }
            }

            return new FluentEntityMapper<T>(this);
        }


    }


}

