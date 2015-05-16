package com.rolonews.hbasemapper;

import static junit.framework.Assert.*;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.rolonews.hbasemapper.mapping.EntityMapper;
import com.rolonews.hbasemapper.mapping.FluentEntityMapper;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author maamria
 */
public class NestedObjectFluentMappingTest extends BaseTest{

    @Test
    public void testMappingWithNestedObject(){
        EntityMapper<BarWithNestedBar> mapper =FluentEntityMapper.builder(BarWithNestedBar.class, "BarWithNestedBar")
                .withRowKeyGenerator(new Function<BarWithNestedBar, String>() {

                    @Override
                    public String apply(BarWithNestedBar barWihtNestedBar) {
                        return String.valueOf(barWihtNestedBar.counter);
                    }
                })
                .withColumnQualifier("info", "counter", "counter")
                .withColumnQualifier("ainfo", "name", "nestedBar.name")
                .withColumnQualifier("ainfo", "date", "nestedBar.date")
                .withCollection("ainfo", "items", "nestedBar.list")
                .build();
        DataStore<BarWithNestedBar> dataStore = DataStoreFactory.getDataStore(BarWithNestedBar.class, getMiniClusterConnection(), mapper);
        BarWithNestedBar bar = BarWithNestedBar.getInstance(NestedBar.getInstance("nestedBar", 12121212L, "item1", "item2", "item3"), 20);
        dataStore.put(bar);
        Optional<BarWithNestedBar> optional = dataStore.get("20");
        BarWithNestedBar actualBar = optional.get();
        assertEquals(20, actualBar.counter);
        assertEquals("nestedBar", actualBar.nestedBar.name);
        assertEquals(12121212l, actualBar.nestedBar.date);
    }


    static class BarWithNestedBar {

        private NestedBar nestedBar;

        private int counter;

        static BarWithNestedBar getInstance(NestedBar nestedBar, int counter){
            BarWithNestedBar bar = new BarWithNestedBar();
            bar.nestedBar = nestedBar;
            bar.counter = counter;
            return bar;
        }
    }

    static class NestedBar {
        private String name;
        private long date;
        private List<String> list;

        static NestedBar getInstance(String name, long date, String... strings){
            NestedBar bar = new NestedBar();
            bar.name = name;
            bar.date = date;
            if(strings != null) {
                bar.list = Arrays.asList(strings);
            }
            return bar;
        }
    }

}
