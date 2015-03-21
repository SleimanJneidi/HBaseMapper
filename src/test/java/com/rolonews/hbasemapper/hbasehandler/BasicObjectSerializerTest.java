package com.rolonews.hbasemapper.hbasehandler;

import com.rolonews.hbasemapper.serialisation.BasicObjectSerializer;
import com.rolonews.hbasemapper.serialisation.ObjectSerializer;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;
/**
 *
 * Created by Sleiman on 10/12/2014.
 *
 */
public class BasicObjectSerializerTest {

    @Test
    public void testSerialization(){
        ObjectSerializer serializer = new BasicObjectSerializer();

        byte []intBuffer = serializer.serialize(1);
        int intValue = serializer.deserialize(intBuffer,Integer.class);
        assertEquals(1,intValue);

        byte []longBuffer = serializer.serialize(1L);
        long longValue = serializer.deserialize(longBuffer,Long.class);
        assertEquals(1L,longValue);

        byte []doubleBuffer = serializer.serialize(1.0);
        double doubleValue = serializer.deserialize(doubleBuffer,Double.class);
        assertTrue(doubleValue == 1.0);

        byte []shortBuffer = serializer.serialize((short)1);
        short shortValue = serializer.deserialize(shortBuffer,Short.class);
        assertEquals((short)1,shortValue);

        byte []stringBuffer = serializer.serialize("Hello World!!!");
        String stringValue = serializer.deserialize(stringBuffer,String.class);
        assertEquals("Hello World!!!",stringValue);


        byte []booleanBuffer = serializer.serialize(false);
        boolean booleanValue = serializer.deserialize(booleanBuffer,Boolean.class);
        assertEquals(false,booleanValue);

        byte []bigDecimalBuffer = serializer.serialize(BigDecimal.ONE);
        BigDecimal bigDecimalValue = serializer.deserialize(bigDecimalBuffer,BigDecimal.class);
        assertEquals(BigDecimal.ONE,bigDecimalValue);

        byte[]byteBufferArray = serializer.serialize(ByteBuffer.wrap(new byte[]{1,2,3}));
        ByteBuffer byteBuffer = serializer.deserialize(byteBufferArray,ByteBuffer.class);
        assertArrayEquals(new byte[]{1,2,3},byteBuffer.array());
    }
}
