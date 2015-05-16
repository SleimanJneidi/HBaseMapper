package com.rolonews.hbasemapper.serialisation;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

public class BasicObjectSerializerTest {

	private BasicObjectSerializer serializer;
	
	@Before
	public void before(){
		serializer = new BasicObjectSerializer();
	}
	
	@Test
	public void testSerialisationForInt(){
		byte[] intBuffer = serializer.serialize(1);
		int intValue = serializer.deserialize(intBuffer, Integer.class);
		assertEquals(1, intValue);
	}
	
	@Test
	public void testSerialisationForLong(){
		byte[] longBuffer = serializer.serialize(1L);
		long longValue = serializer.deserialize(longBuffer, Long.class);
		assertEquals(1L, longValue);
	}
	
	@Test
	public void testSerialisationForFloat(){
		byte[] floatBuffer = serializer.serialize(1.0f);
		float floatValue = serializer.deserialize(floatBuffer, Float.class);
		assertTrue(1.0f == floatValue);
	}
	
	@Test
	public void testSerialisationForDouble(){
		byte[] doubleBuffer = serializer.serialize(1.0);
		double doubleValue = serializer.deserialize(doubleBuffer, Double.class);
		assertTrue(doubleValue == 1.0);
	}
	
	@Test
	public void testSerialisationForShort(){
		byte[] shortBuffer = serializer.serialize((short) 1);
		short shortValue = serializer.deserialize(shortBuffer, Short.class);
		assertEquals((short) 1, shortValue);
	}
	
	@Test
	public void testSerialisationForString(){
		byte[] stringBuffer = serializer.serialize("Hello World!!!");
		String stringValue = serializer.deserialize(stringBuffer, String.class);
		assertEquals("Hello World!!!", stringValue);
	}
	
	@Test
	public void testSerialisationForBoolean(){
		byte[] booleanBuffer = serializer.serialize(false);
		boolean booleanValue = serializer.deserialize(booleanBuffer,
				Boolean.class);
		assertEquals(false, booleanValue);
	}
	
	@Test
	public void testSerialisationForBigDecimal(){
		byte[] bigDecimalBuffer = serializer.serialize(BigDecimal.ONE);
		BigDecimal bigDecimalValue = serializer.deserialize(bigDecimalBuffer,
				BigDecimal.class);
		assertEquals(BigDecimal.ONE, bigDecimalValue);
	}
	
	@Test
	public void testSerialisationForByteBuffer(){
		byte[] byteBufferArray = serializer.serialize(ByteBuffer
				.wrap(new byte[] { 1, 2, 3 }));
		ByteBuffer byteBuffer = serializer.deserialize(byteBufferArray,
				ByteBuffer.class);
		assertArrayEquals(new byte[] { 1, 2, 3 }, byteBuffer.array());
	}
	
	@Test
	public void testDefaultSerializationForEnum() {
		byte[] enumBufferArray = serializer.serialize(TestEnum.TEST_ENUM_VALUE_1);
		TestEnum enumValue = serializer.deserialize(enumBufferArray, TestEnum.class);
		assertEquals(TestEnum.TEST_ENUM_VALUE_1, enumValue);
		
		enumBufferArray = serializer.serialize(TestEnum.TEST_ENUM_VALUE_3);
		enumValue = serializer.deserialize(enumBufferArray, TestEnum.class);
		assertEquals(TestEnum.TEST_ENUM_VALUE_3, enumValue);
	}
	
	private static enum TestEnum {
		TEST_ENUM_VALUE_1, TEST_ENUM_VALUE_2, TEST_ENUM_VALUE_3
	}

}
