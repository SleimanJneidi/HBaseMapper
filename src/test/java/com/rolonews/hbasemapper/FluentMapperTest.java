package com.rolonews.hbasemapper;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.google.common.base.Function;
import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import com.rolonews.hbasemapper.mapping.EntityMapper;
import com.rolonews.hbasemapper.mapping.FluentEntityMapper;
/**
 * Created by Sleiman on 25/01/2015.
 */
public class FluentMapperTest extends BaseTest{
	
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
    
    @Test(expected = InvalidMappingException.class)
    public void testGenericClassMapping(){
    	FluentEntityMapper.builder(ParamBar.class, "ParamBar");
    }
    
    

    static class Bar{
        private String id;
        private String name;
    }
    
    static class ParamBar<T> {
    	private T t;
    	private String name;
    }
    
    

}
