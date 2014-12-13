package com.rolonews.hbasemapper.hbasehandler;

import static com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler.StringConverter.*;

import org.apache.commons.lang.NotImplementedException;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;


/**
 *
 * Created by Sleiman on 13/12/2014.
 */
public class StringConverterTest {

    @Test
    public void testCanConvertTypesFromString(){

        assertEquals(1, convert(Integer.class,"1"));
        assertEquals(1.0, convert(Double.class,"1.0"));
        assertEquals(BigDecimal.ONE,convert(BigDecimal.class,"1"));
        assertEquals(12L,convert(Long.class,"12"));
        assertEquals(Short.valueOf("4"),convert(Short.class,"4"));
        assertEquals(true,convert(Boolean.class,"true"));


    }

    @Test(expected = NotImplementedException.class)
    public void testShouldFailIfTheTypeCannotBeConverted(){
        convert(ByteBuffer.class,"1");
    }
}
