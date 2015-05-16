package com.rolonews.hbasemapper.mapping;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.rolonews.hbasemapper.Consumer;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import com.rolonews.hbasemapper.serialisation.ObjectSerializer;
import com.rolonews.hbasemapper.serialisation.SerialisationManager;
import com.rolonews.hbasemapper.serialisation.SerializerRegistry;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * Created by Sleiman on 25/01/2015.
 */

public class FluentEntityMapper<T> implements EntityMapper<T> {

    private final Class<T> clazz;
    private final Map<HCellDescriptor, FieldDescriptor> columns;
    private final HTableDescriptor tableDescriptor;
    private final Function<T,?> rowKeyGenerator;
    private final Consumer<Pair<T, byte[]>> rowKeyConsumer;
    private final Predicate<T> validator;

    private final SerialisationManager serialisationManager;

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
    public Consumer<Pair<T, byte[]>> rowKeyConsumer() {
        return this.rowKeyConsumer;
    }

    public SerialisationManager serializationManager() {
		return serialisationManager;
	}

    @Override
    public Predicate<T> validator() { return validator; }

    @Override
    public Map<HCellDescriptor, FieldDescriptor> columns() {
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
        if(builder.rowKeyConsumer == null){
            this.rowKeyConsumer = getEmptyConsumer();
        }
        else {
            this.rowKeyConsumer = builder.rowKeyConsumer;
        }
        this.serialisationManager = builder.serializationRegistry.getSerialisationManager();

        if(builder.validator==null){
            this.validator = getTruePredicate();
        }else{
            this.validator = builder.validator;
        }
    }

    public static <T> Builder<T> builder(Class<T> clazz, String tableName){
        return new Builder<T>(clazz,tableName);
    }

    public static class Builder<T>{

        private final Set<String> columnFamilies = new HashSet<String>();
        private String tableName;
        private Class<T> clazz;
        private Function<T,?> rowKeyGenerator;
        private Consumer<Pair<T, byte[]>> rowKeyConsumer;
        private Predicate<T> validator;
        private Map<HCellDescriptor, FieldDescriptor> columns;
        
        private SerializerRegistry serializationRegistry;

        public Builder<T> withRowKeyGenerator(Function<T,?> rowKeyGenerator){
            this.rowKeyGenerator = rowKeyGenerator;
            return this;
        }

        public Builder<T> withRowKeyConsumer(Consumer<Pair<T, byte[]>> rowKeyConsumer){
            this.rowKeyConsumer = rowKeyConsumer;
            return this;
        }

        public Builder<T> withColumnQualifier(String family,String qualifier, String fieldName){
            columnFamilies.add(family);
            addCellDescriptor(fieldName, family, qualifier, false);
            return this;
        }
        
        public Builder<T> withCollection(String family, String qualifier, String fieldName){
        	columnFamilies.add(family);
            addCellDescriptor(fieldName, family, qualifier, true);
        	return this;
        }
        
        public <U> Builder<T> withSerialiser(Class<U> type, ObjectSerializer<U> serializer){
        	serializationRegistry.addSerializer(type, serializer);
        	return this;
        }

        public Builder<T> withValidator(Predicate<T> validator){
            this.validator = validator;
            return this;
        }

        public Builder(Class<T> clazz, String tableName){
            Preconditions.checkNotNull(clazz);
            Preconditions.checkNotNull(tableName);
            if(clazz.getTypeParameters().length != 0){
            	throw new InvalidMappingException("Cannot map generic types");
            }
            this.clazz = clazz;
            this.tableName = tableName;
            this.serializationRegistry = new SerializerRegistry();
            this.columns = new HashMap<HCellDescriptor, FieldDescriptor>();
        }

        public EntityMapper<T> build(){
            Preconditions.checkArgument(StringUtils.isNotBlank(tableName));
            Preconditions.checkNotNull(rowKeyGenerator);
            Preconditions.checkArgument(columnFamilies.size()>=1);
            return new FluentEntityMapper<T>(this);
        }

        private void addCellDescriptor(String fieldName, String family, String qualifier, boolean isCollection){
        	Preconditions.checkNotNull(fieldName);
        	Preconditions.checkNotNull(family);
    		Preconditions.checkNotNull(qualifier);
			columns.put(new HCellDescriptor(family,qualifier, isCollection), FieldDescriptor.of(fieldName, clazz, isCollection));
    	}
    }

	@Override
	public EntityMapper<T> register() {
		MappingRegistry.register(this);
		return this;
	}

    private static <U> Predicate<U> getTruePredicate(){
        return new Predicate<U>() {
            @Override
            public boolean apply(U input) {
                return true;
            }
        };
    }

    private static <U> Consumer<Pair<U, byte[]>> getEmptyConsumer(){
        return new Consumer<Pair<U, byte[]>>() {
            @Override
            public void consume(Pair<U, byte[]> uPair) {

            }
        };
    }


	
}



