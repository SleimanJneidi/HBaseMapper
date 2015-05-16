package com.rolonews.hbasemapper.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.serialisation.ObjectSerializer;
import com.rolonews.hbasemapper.serialisation.SerialisationManager;
import com.rolonews.hbasemapper.serialisation.SerializerRegistry;

public class RowGetter {

	private final Projector projector;
	
	public static Projector projector(HTableInterface tableInterface){
		Preconditions.checkNotNull(tableInterface);
		
		return new Projector(tableInterface);
		
	}
	
	private final SerialisationManager serialisationManager;
	
	private RowGetter(Projector projector){
		this.projector = projector;
		this.serialisationManager = projector.serializationRegistry.getSerialisationManager();
	}
	
	public <K> Optional<Map<String,byte[]>> get(K rowKey) throws IOException{
		Preconditions.checkNotNull(rowKey);
		
		byte []rowKeyBytes = serialisationManager.serialize(rowKey);
		
		Map<String, Pair<byte[],byte[]>>projectionMap = this.projector.projectonMap;
		Get get = constructGet(rowKeyBytes, projectionMap);
		
		Result result = this.projector.tableInterface.get(get);
		
		if(result.isEmpty()){
			return Optional.<Map<String,byte[]>>absent();
		}
		Map<String,byte[]> resultMap = new HashMap<String, byte[]>();
		
		for(String key: projectionMap.keySet()){
			byte[] value = result.getValue(projectionMap.get(key).getFirst(), projectionMap.get(key).getSecond());
			resultMap.put(key, value);
		}
		
		return Optional.of(resultMap);
	}
	
	private Get constructGet(byte[] rowKey, Map<String, Pair<byte[],byte[]>>projectonMap){
		Get get  = new Get(rowKey);
		for(Pair<byte[],byte[]> column:projectonMap.values()){
			get.addColumn(column.getFirst(),column.getSecond());
		}
		return get;
	}
	
	public static class Projector{
		
		private final HTableInterface tableInterface;
		private final Map<String, Pair<byte[],byte[]>> projectonMap;
		private final SerializerRegistry serializationRegistry;
		
		public Projector(HTableInterface tableInterface){
			this.tableInterface = tableInterface;
			this.projectonMap = new HashMap<String, Pair<byte[],byte[]>>();
			this.serializationRegistry = new SerializerRegistry();
		}
		
		public Projector withColumns(String family,String qualifier){
			Preconditions.checkNotNull(family);
			Preconditions.checkNotNull(qualifier);
			
			byte[] familyBytes = Bytes.toBytes(family);
			byte[] qualiBytes = Bytes.toBytes(qualifier);
			Pair<byte[],byte[]> familyQualifierPair = new Pair<byte[], byte[]>(familyBytes, qualiBytes);
			
			String familyQualifierKey = family+"."+qualifier;
			this.projectonMap.put(familyQualifierKey, familyQualifierPair);
			
			return this;
		}
		
		public <T> Projector withSerializer(Class<T> clazz, ObjectSerializer<T> serializer){
			Preconditions.checkNotNull(clazz);
			Preconditions.checkNotNull(serializer);
			serializationRegistry.addSerializer(clazz, serializer);
			return this;
		}
		
		public RowGetter build(){
			RowGetter getter = new RowGetter(this);
			return getter;
		}
	}
}
