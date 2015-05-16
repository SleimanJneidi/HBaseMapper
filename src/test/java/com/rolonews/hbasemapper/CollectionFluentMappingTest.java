package com.rolonews.hbasemapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.rolonews.hbasemapper.mapping.EntityMapper;
import com.rolonews.hbasemapper.mapping.FluentEntityMapper;

/**
 * @author maamria
 */
public class CollectionFluentMappingTest extends BaseTest{

	@Test
    public void testListMapping(){
    	EntityMapper<BarWithList> mapper = FluentEntityMapper.builder(BarWithList.class, "BarWithList")
    		.withColumnQualifier("info", "number", "number")
    		.withRowKeyGenerator(barWithListKey())
    		.withCollection("info", "list", "list").build();
    	
    	DataStore<BarWithList> dataStore = DataStoreFactory.getDataStore(BarWithList.class, getMiniClusterConnection(), mapper);
    	BarWithList barWithList = new BarWithList(18,  "str1", "str2", "str3");
		dataStore.put(barWithList);
    	Optional<BarWithList> optional = dataStore.get("18");
    	BarWithList actualBarWithList = optional.get();
		assertEquals(barWithList.number, actualBarWithList.number);
		assertEquals(barWithList.list, actualBarWithList.list);
    }
	
	@Test
    public void testSetMapping(){
    	EntityMapper<BarWithSet> mapper = FluentEntityMapper.builder(BarWithSet.class, "BarWithSet")
    		.withColumnQualifier("info", "byteProperty", "byteProperty")
    		.withRowKeyGenerator(barWithSetKey())
    		.withCollection("info", "myset", "set").build();
    	
    	DataStore<BarWithSet> dataStore = DataStoreFactory.getDataStore(BarWithSet.class, getMiniClusterConnection(), mapper);
    	BarWithSet barWithSet = new BarWithSet();
    	barWithSet.byteProperty = 112;
    	barWithSet.set = Sets.newHashSet("item1", "item2", "item3");
		dataStore.put(barWithSet);
    	Optional<BarWithSet> optional = dataStore.get("112");
    	BarWithSet actualBarWithSet = optional.get();
		assertEquals(barWithSet.byteProperty, actualBarWithSet.byteProperty);
		assertTrue(barWithSet.set.containsAll(actualBarWithSet.set));
		assertTrue(actualBarWithSet.set.containsAll(barWithSet.set));
    }
    
    @Test
    public void testMapMapping(){
    	EntityMapper<BarWithMap> mapper = FluentEntityMapper.builder(BarWithMap.class, "BarWithMap")
    			.withRowKeyGenerator(barWithMapKey())
    			.withColumnQualifier("info", "long", "longProperty")
    			.withCollection("info", "embeddedMap", "map")
    			.build();
    	DataStore<BarWithMap> dataStore = DataStoreFactory.getDataStore(BarWithMap.class, getMiniClusterConnection(), mapper);
    	BarWithMap barWithMap = new BarWithMap();
    	barWithMap.longProperty = 101010L;
    	barWithMap.map = new LinkedHashMap<String, Integer>();
    	barWithMap.map.put("key1", 12);
    	barWithMap.map.put("key2", 11);
    	barWithMap.map.put("key3", 32);
    	barWithMap.map.put("key4", 54);
    	barWithMap.map.put("key5", 98);
    	dataStore.put(barWithMap);
    	Optional<BarWithMap> optional = dataStore.get("101010");
    	BarWithMap getBarWithMap = optional.get();
		assertNotNull(getBarWithMap);
    	assertEquals(101010L, getBarWithMap.longProperty);
    }
    
    @Test
    public void testCollectionMapping(){
    	EntityMapper<ComplexBar> mapper = FluentEntityMapper.builder(ComplexBar.class, "ComplexBar")
    			.withRowKeyGenerator(complexBarKey())
    			.withColumnQualifier("info1", "property1", "stringProperty")
    			.withColumnQualifier("info1", "property2", "intProperty")
    			.withCollection("info1", "property3", "barSet")
    			.withCollection("info1", "property4", "barMap")
    			.withCollection("info2", "property5", "barList")
    			.withCollection("info3", "property6", "barLinkedList")
    			.withCollection("info3", "property7", "sortedByteSet")
    			.build();
    	DataStore<ComplexBar> dataStore = DataStoreFactory.getDataStore(ComplexBar.class, getMiniClusterConnection(), mapper);
    	ComplexBar complexBar = new ComplexBar();
    	complexBar.intProperty = 234;
    	complexBar.stringProperty = "dummyStr";
    	complexBar.barList = Lists.newArrayList(1, 2, 3, 10, 101);
    	complexBar.barLinkedList = new LinkedList<String>();
    	complexBar.barLinkedList.add("item1");
    	complexBar.barLinkedList.add("item10");
    	complexBar.barLinkedList.add("item100");
    	complexBar.barMap = new LinkedHashMap<String, Integer>();
    	complexBar.barMap.put("barMap1", 2);
    	complexBar.barMap.put("barMap2", 4);
    	complexBar.barMap.put("barMap3", 16);
    	complexBar.barMap.put("barMap4", 32);
    	complexBar.barMap.put("barMap5", 64);
    	complexBar.barSet = new HashSet<String>();
    	complexBar.barSet.add("barSet100");
    	complexBar.barSet.add("barSet200");
    	complexBar.barSet.add("barSet300");
    	complexBar.barSet.add("barSet400");
    	complexBar.sortedByteSet = new TreeSet<Byte>();
    	complexBar.sortedByteSet.add((byte) 114);
    	complexBar.sortedByteSet.add((byte) 77);
    	complexBar.sortedByteSet.add((byte) 33);
    	complexBar.sortedByteSet.add((byte) 40);
    	dataStore.put(complexBar);
    	Optional<ComplexBar> optional = dataStore.get("dummyStr");
    	ComplexBar actualBar = optional.get();
    	
		assertNotNull(actualBar);
		
		assertEquals(complexBar.intProperty, actualBar.intProperty);
		assertEquals(complexBar.stringProperty, actualBar.stringProperty);
		
		assertTrue(complexBar.barLinkedList.containsAll(actualBar.barLinkedList));
		assertTrue(actualBar.barLinkedList.containsAll(complexBar.barLinkedList));
		
		assertTrue(complexBar.barList.containsAll(actualBar.barList));
		assertTrue(actualBar.barList.containsAll(complexBar.barList));
		
		assertTrue(complexBar.barSet.containsAll(actualBar.barSet));
		assertTrue(actualBar.barSet.containsAll(complexBar.barSet));
		
		assertTrue(complexBar.sortedByteSet.containsAll(actualBar.sortedByteSet));
		assertTrue(actualBar.sortedByteSet.containsAll(complexBar.sortedByteSet));
		
		assertTrue(complexBar.barMap.keySet().containsAll(actualBar.barMap.keySet()));
		assertTrue(actualBar.barMap.keySet().containsAll(complexBar.barMap.keySet()));
		
		assertTrue(complexBar.barMap.values().containsAll(actualBar.barMap.values()));
		assertTrue(actualBar.barMap.values().containsAll(complexBar.barMap.values()));
    }
    
    static class BarWithList {
    	private List<String> list;
    	private int number;
    	
    	private BarWithList(){
    		
    	}
    	
    	public BarWithList(int number, String... list) {
			this.number = number;
			this.list = Lists.newArrayList(list);
		}
    }
    
    static Function<BarWithList, String> barWithListKey(){
		return new Function<BarWithList, String>() {

			@Override
			public String apply(BarWithList bar) {
				return String.valueOf(bar.number);
			}
		};
	}
    
    static class BarWithMap {
    	
    	private Map<String, Integer> map;
    	
    	private long longProperty;
    }
    
    static Function<BarWithMap, String> barWithMapKey(){
		return new Function<BarWithMap, String>() {

			@Override
			public String apply(BarWithMap bar) {
				return String.valueOf(bar.longProperty);
			}
		};
	}
    
    static class BarWithSet {
    	
    	private Set<String> set;
    	
    	private byte byteProperty;
    }
    
    static Function<BarWithSet, String> barWithSetKey(){
    	return new Function<BarWithSet, String>() {

			@Override
			public String apply(BarWithSet bar) {
				return String.valueOf(bar.byteProperty);
			}
		};
    }
    
    static class ComplexBar {
    	private Set<String> barSet;
    	private Map<String, Integer> barMap;
    	private List<Integer> barList;
    	private LinkedList<String> barLinkedList;
    	private SortedSet<Byte> sortedByteSet;
    	private String stringProperty;
    	private int intProperty;
    }
	
    static Function<ComplexBar, String> complexBarKey(){
    	return new Function<ComplexBar, String>() {

			@Override
			public String apply(ComplexBar bar) {
				return bar.stringProperty;
			}
		};
    }
}
