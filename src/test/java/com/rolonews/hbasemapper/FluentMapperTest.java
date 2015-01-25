package com.rolonews.hbasemapper;

import org.junit.Test;

import static org.junit.Assert.*;
/**
 * Created by Sleiman on 25/01/2015.
 */
public class FluentMapperTest {

    @Test
    public void testValidMapping(){

        EntityMapper<Bar> mapper = FluentEntityBuilder.builder(Bar.class,"bar")
                .withRowKey("id")
                .withRowKey("name")
                .withRowKeySeparator("$")
                .withColumnQualifier("info","name","name").build();

        assertEquals(Bar.class, mapper.clazz());
        assertEquals("bar",mapper.table().name());
        assertEquals(1, mapper.columns().size());
        assertEquals(2, mapper.table().rowKey().length);
        assertEquals("$",mapper.table().rowKeySeparator());
    }

    class Bar{
        private String id;
        private String name;
    }

}
