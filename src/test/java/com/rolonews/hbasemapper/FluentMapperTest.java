package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.rolonews.hbasemapper.mapping.EntityMapper;
import com.rolonews.hbasemapper.mapping.FluentEntityMapper;

import org.junit.Test;

import javax.annotation.Nullable;

import static org.junit.Assert.*;
/**
 * Created by Sleiman on 25/01/2015.
 */
public class FluentMapperTest {

    @Test
    public void testValidMapping(){

        EntityMapper<Bar> mapper = FluentEntityMapper.builder(Bar.class, "bar")
                .withRowKeyGenerator(new Function<Bar, Object>() {

                    @Override
                    public Object apply(Bar input) {
                        return 1;
                    }
                })
                .withColumnQualifier("info", "name", "name").build();

        assertEquals(Bar.class, mapper.clazz());
        assertEquals("bar",mapper.tableDescriptor().getTableName().getNameAsString());
        assertEquals(1, mapper.columns().size());

    }

    class Bar{
        private String id;
        private String name;
    }

}
