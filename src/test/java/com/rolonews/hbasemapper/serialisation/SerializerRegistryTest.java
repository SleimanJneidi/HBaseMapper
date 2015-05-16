package com.rolonews.hbasemapper.serialisation;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

public class SerializerRegistryTest {

	private SerialisationManager serialisationManager;
	
	@Before
	public void before(){
		serialisationManager = new SerialisationManager(Collections.<Class<?>, ObjectSerializer<?>>emptyMap());
	}
	
	@Test
	public void testSerialiseBasicTypes(){
		BasicObjectSerializer basicObjectSerializer = new BasicObjectSerializer();
		assertArrayEquals(basicObjectSerializer.serialize("Test"), serialisationManager.serialize("Test"));
		assertArrayEquals(basicObjectSerializer.serialize(12), serialisationManager.serialize(12));
		assertArrayEquals(basicObjectSerializer.serialize(120.0f), serialisationManager.serialize(120.0f));
		assertArrayEquals(basicObjectSerializer.serialize(true), serialisationManager.serialize(true));
		assertArrayEquals(basicObjectSerializer.serialize(TestEnum.TEST_ENUM_1), serialisationManager.serialize(TestEnum.TEST_ENUM_1));
	}
	
	@Test
	public void testDeserialiseBasicTypes(){
		BasicObjectSerializer basicObjectSerializer = new BasicObjectSerializer();
		assertEquals("Test", serialisationManager.deserialize(basicObjectSerializer.serialize("Test"), String.class));
		assertEquals(TestEnum.TEST_ENUM_2, serialisationManager.deserialize(basicObjectSerializer.serialize(TestEnum.TEST_ENUM_2), TestEnum.class));
		assertEquals(false, serialisationManager.deserialize(basicObjectSerializer.serialize(false), Boolean.class));
	}
	
	@Test
	public void testSerialise(){
		ObjectSerializer<TestClass> testClassSerialiser = new ObjectSerializer<SerializerRegistryTest.TestClass>() {
			
			@Override
			public byte[] serialize(TestClass object,
					SerialisationManager serialisationManager) {
				String text = object.getTestProperty1() + "\n" + object.getTestProperty2();
				return serialisationManager.serialize(text);
			}
			
			@Override
			public TestClass deserialize(byte[] buffer, Class<TestClass> clazz,
					SerialisationManager serialisationManager) {
				String text = serialisationManager.deserialize(buffer, String.class);
				List<String> splitter = Lists.newArrayList(Splitter.on("\n").split(text));
				int property1 = Integer.valueOf(splitter.get(0));
				String property2 = splitter.get(1);
				return new TestClass(property1, property2);
			}
		} ;
		LinkedHashMap<Class<?>, ObjectSerializer<?>> serialisers = new LinkedHashMap<Class<?>, ObjectSerializer<?>>();
		serialisers.put(TestClass.class, testClassSerialiser);
		SerialisationManager managerWithCustomSerialiser = new SerialisationManager(serialisers);
		TestClass testObject = new TestClass(12, "Test");
		byte[] serialized = managerWithCustomSerialiser.serialize(testObject);
		assertEquals(testObject, managerWithCustomSerialiser.deserialize(serialized, TestClass.class));
		
	}
	
	private static class TestClass {
		private int testProperty1;
		private String testProperty2;
		
		private TestClass(int testProperty1, String testProperty2){
			this.testProperty1 = testProperty1;
			this.testProperty2 = testProperty2;
		}
		
		public int getTestProperty1() {
			return testProperty1;
		}
		
		public String getTestProperty2() {
			return testProperty2;
		}
		
		@Override
		public boolean equals(Object obj) {
			if(obj instanceof TestClass){
				TestClass other = (TestClass) obj;
				return Objects.equal(testProperty1, other.testProperty1) 
						&& Objects.equal(testProperty2, other.testProperty2);
			}
			return false;
		}
		
		@Override
		public int hashCode() {
			return Objects.hashCode(testProperty1, testProperty2);
		}
	}
	
	private static enum TestEnum {
		TEST_ENUM_1, TEST_ENUM_2
	}
	
}
