package com.rolonews.hbasemapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.rolonews.hbasemapper.mapping.HTableHandler;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HTableHandlerTest extends BaseTest{

    @Mock
    private HTableInterface table;

    @Mock
    private HConnection connection;

    @Captor
    private ArgumentCaptor putCaptor;

    @Mock
    private Configuration configuration;


    @Test
    public void testInsertRecord() throws Exception {



        TableName tableName = TableName.valueOf("Person");
        when(connection.isTableAvailable(tableName)).thenReturn(true);
        when(connection.getTable(tableName)).thenReturn(table);
        when(connection.getConfiguration()).thenReturn(configuration);

        HTableHandler tableHandler = new HTableHandler(connection);
        HTableInterface  hTableInterface =  tableHandler.getOrCreateHTable(Person.class);
        assertNotNull(hTableInterface);


    }
}
