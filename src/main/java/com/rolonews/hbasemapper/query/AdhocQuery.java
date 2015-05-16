package com.rolonews.hbasemapper.query;

import com.google.common.base.Preconditions;
import com.rolonews.hbasemapper.serialisation.BasicObjectSerializer;
import com.rolonews.hbasemapper.serialisation.SerialisationManager;
import com.rolonews.hbasemapper.serialisation.SerializerRegistry;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Sleiman
 *
 */
public class AdhocQuery {
	
	private final Builder queryBuilder;
	
	private final SerialisationManager serialisationManager;
	
	private AdhocQuery(Builder queryBuilder){
		this.queryBuilder = queryBuilder;
		this.serialisationManager = queryBuilder.serializationRegistry.getSerialisationManager();
	}
	
	public static Builder builder(HTableInterface table){
		Preconditions.checkNotNull(table);
		Builder builder = new Builder(table);
		return builder;
	}
	
	public HTableInterface table(){
		return this.queryBuilder.table;
	}
	
	public <K> List<QueryResult<K,Map<String,byte[]>>> execute(Class<K> rowKeyType) throws IOException{
		Scan scan = this.queryBuilder.scan;
		HTableInterface table = this.queryBuilder.table;
		ResultScanner resultScanner = null;
		List<QueryResult<K,Map<String,byte[]>>> queryResult = new ArrayList<QueryResult<K,Map<String,byte[]>>>();
		
		try{
			resultScanner = table.getScanner(scan);
			for(Result result: resultScanner){
				
				byte[] row = result.getRow();
				
				K deserializeRowKey = serialisationManager.deserialize(row, rowKeyType);
				
				Map<String,byte[]> rowCells = new HashMap<String,byte[]>();
		
				Map<String, Pair<byte[], byte[]>> projectonMap = this.queryBuilder.projectonMap;
				
				for(String key: projectonMap.keySet()){
					Pair<byte[], byte[]> familyQualPair = projectonMap.get(key);
					byte[] value = result.getValue(familyQualPair.getFirst(), familyQualPair.getSecond());
					rowCells.put(key, value);
				}
				
				QueryResult<K,Map<String,byte[]>> rowQueryResult = new QueryResult<K, Map<String,byte[]>>(deserializeRowKey, rowCells);
				queryResult.add(rowQueryResult);
				
				
			}
			
		}finally{
			if(resultScanner!=null){
				resultScanner.close();
			}
		}
		
		return queryResult;
	}
	
	public static class Builder{
		
		private final HTableInterface table;
		private final Scan scan;
		private final FilterList filterList;
		private final Map<String, Pair<byte[],byte[]>> projectonMap;
		private final BasicObjectSerializer basicObjectSerializer;
		private final SerializerRegistry serializationRegistry;
		
		public Builder(HTableInterface table){
			this.table  = table;
			this.scan = new Scan();
			this.filterList = new FilterList();
			projectonMap = new HashMap<String, Pair<byte[],byte[]>>();
			serializationRegistry = new SerializerRegistry();
			basicObjectSerializer = new BasicObjectSerializer();
		}
		
		public Builder withStartRow(Object startRow){
			Preconditions.checkNotNull(startRow);
			byte[] rowKey = basicObjectSerializer.serialize(startRow);
			scan.setStartRow(rowKey);
			return this;
		}
		
		public Builder withStopRow(Object stopRow){
			Preconditions.checkNotNull(stopRow);
			byte[] rowKey = basicObjectSerializer.serialize(stopRow);
			scan.setStopRow(rowKey);
			return this;
		}
		
		public Builder withCaching(int caching){
			scan.setCaching(caching);
			return this;
		}
		
		public Builder withMaxResultSize(int maxResultSize){
			scan.setMaxResultSize(maxResultSize);
			return this;
		}
		
		public Builder withSingleColumnValueFilter(String family,String qualifier, CompareOp compareOp, Object value){
			byte[] valueBytes = basicObjectSerializer.serialize(value);
			Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family),Bytes.toBytes(qualifier), compareOp, valueBytes);
			this.filterList.addFilter(filter);
			
			return this;
		}
		
		public Builder withPageFilter(long pageSize){
			Filter filter  = new PageFilter(pageSize);
			this.filterList.addFilter(filter);
			return this;
		}
		
		public Builder withColumns(String family,String qualifier){
			Preconditions.checkNotNull(family);
			Preconditions.checkNotNull(qualifier);
			
			byte[] familyBytes = Bytes.toBytes(family);
			byte[] qualiBytes = Bytes.toBytes(qualifier);
			scan.addColumn(familyBytes,qualiBytes);
			Pair<byte[],byte[]> familyQualifierPair = new Pair<byte[], byte[]>(familyBytes, qualiBytes);
			
			String familyQualifierKey = family+"."+qualifier;
			this.projectonMap.put(familyQualifierKey, familyQualifierPair);
			
			return this;
		}
		
		public AdhocQuery build(){
			if(projectonMap.isEmpty()){
				this.filterList.addFilter(new KeyOnlyFilter());
				this.filterList.addFilter(new FirstKeyOnlyFilter());
			}
			if(this.filterList.getFilters().size() > 0){
				this.scan.setFilter(this.filterList);
			}
			final AdhocQuery query = new AdhocQuery(this);
			return query;
		}
		
	}

}
