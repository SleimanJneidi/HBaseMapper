package com.rolonews.hbasemapper;

import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.annotations.Column;
import com.rolonews.hbasemapper.annotations.Table;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import com.rolonews.hbasemapper.utils.ReflectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Triple;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.*;

/**
 *
 * Created by Sleiman on 25/01/2015.
 */

class FluentEntityBuilder<T> implements EntityMapper<T> {

    private final Class<T> clazz;
    private final Table table;
    private final Map<String, Field> rowKeys;
    private final Map<Column, Field> columns;

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

    private FluentEntityBuilder(Builder<T> builder){
        this.clazz = builder.clazz;
        this.table = builder.table;
        this.rowKeys = builder.rowKeys;
        this.columns = builder.columns;
    }

    public static <T> Builder<T> builder(Class<T> clazz, String tableName){
        Builder<T> mapperBuilder = new Builder<T>(clazz,tableName);
        return mapperBuilder;
    }

    public static class Builder<T>{

        private String tableName;
        private Class<T> clazz;
        private final Set<String> columnFamilies = new HashSet<String>();
        private List<Field> allFields;
        private List<String> rowKeyFieldsName = new ArrayList<String>();
        private String rowKeySeparator;
        private List<Triple<String,String,String>> columnsFields = new ArrayList<Triple<String, String,String>>();

        private Table table;
        private Map<String, Field> rowKeys;
        private Map<Column, Field> columns;

        public Builder<T> withTable(String tableName){
            this.tableName = tableName;
            return this;
        }

        public Builder<T> withRowKey(String fieldName){
            rowKeyFieldsName.add(fieldName);
            return this;
        }

        public Builder<T> withRowKeySeparator(String separator){
            this.rowKeySeparator = separator;
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

        public FluentEntityBuilder<T> build(){

            Preconditions.checkArgument(StringUtils.isNotBlank(tableName));
            Preconditions.checkArgument(rowKeyFieldsName.size() >= 1);
            Preconditions.checkArgument(columnFamilies.size()>=1);

            this.allFields = ReflectionUtils.getDeclaredAndInheritedFields(clazz);

            final String[] rowKeysCopy = new String[rowKeyFieldsName.size()];

            // copy the list into an array
            for(int i=0;i<rowKeyFieldsName.size();i++){
                rowKeysCopy[i] = rowKeyFieldsName.get(i);
            }

            Arrays.sort(rowKeysCopy);

            rowKeys = new HashMap<String, Field>();

            Map<String, Field> map = ReflectionUtils.getDeclaredAndInheritedFieldsMap(clazz);

            for (String rowKey : rowKeyFieldsName) { // check if there are fields that match row keys
                if (!map.containsKey(rowKey)) {
                    throw new InvalidMappingException(rowKey + " is not a field");
                } else {
                    rowKeys.put(rowKey, map.get(rowKey));
                }
            }

            final String[]familiesArray = new String[columnFamilies.size()];
            final String[]families = columnFamilies.toArray(familiesArray);

            columns = new HashMap<Column, Field>();

            for (Triple<String, String, String> columnsField : columnsFields) {
                final String family = columnsField.getFirst();
                final String qualifier = columnsField.getSecond();
                final String field = columnsField.getThird();

                Column column = new Column(){

                    @Override
                    public Class<? extends Annotation> annotationType() {
                        return Column.class;
                    }

                    @Override
                    public String family() {
                        return family;
                    }

                    @Override
                    public String qualifier() {
                        return qualifier;
                    }
                };

                if(!map.containsKey(field)){
                    throw new InvalidMappingException(field + " is not a field");
                }else{
                    columns.put(column,map.get(field));
                }
            }
            this.table = new Table(){

                @Override
                public Class<? extends Annotation> annotationType() {
                    return Table.class;
                }

                @Override
                public String name() {
                    return tableName;
                }

                @Override
                public String[] rowKey() {
                    return rowKeysCopy;
                }

                @Override
                public String rowKeySeparator() {
                    if(rowKeySeparator==null){
                        try {
                            return  (String)Table.class.getMethod("rowKeySeparator").getDefaultValue();
                        } catch (NoSuchMethodException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    return rowKeySeparator;
                }

                @Override
                public String[] columnFamilies() {
                    return families;
                }
            };

            return new FluentEntityBuilder<T>(this);
        }


    }


}

