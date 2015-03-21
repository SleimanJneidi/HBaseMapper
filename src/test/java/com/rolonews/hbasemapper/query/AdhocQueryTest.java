package com.rolonews.hbasemapper.query;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.rolonews.hbasemapper.BaseTest;

public class AdhocQueryTest extends BaseTest {

    private static HTableInterface testTable;

    @BeforeClass
    public static void setup() throws IOException{
        HConnection connection = BaseTest.getMiniClusterConnection();
        HBaseAdmin admin = new HBaseAdmin(connection);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("AdhocQueryTableTest"));

        tableDescriptor.addFamily(new HColumnDescriptor("firstFamily"));
        tableDescriptor.addFamily(new HColumnDescriptor("secondFamily"));


        admin.createTable(tableDescriptor);
        admin.close();

    }

    @AfterClass
    public static void teardown() throws IOException{
        BaseTest.truncateHBaseTable(testTable.getName().getNameAsString());

    }

    @Before
    public void createTable() throws IOException{

        HConnection connection = BaseTest.getMiniClusterConnection();
        testTable = connection.getTable("AdhocQueryTableTest");

        List<Put> puts = new ArrayList<Put>();

        Put put1 = new Put("row1_1".getBytes());
        put1.add("firstFamily".getBytes(),"age".getBytes(), Bytes.toBytes(12));
        put1.add("firstFamily".getBytes(),"name".getBytes(), Bytes.toBytes("Peter"));
        put1.add("firstFamily".getBytes(),"country".getBytes(), Bytes.toBytes("UK"));
        put1.add("secondFamily".getBytes(),"language".getBytes(),Bytes.toBytes("English"));
        puts.add(put1);

        Put put2 = new Put("row1_2".getBytes());
        put2.add("firstFamily".getBytes(),"age".getBytes(), Bytes.toBytes(24));
        put2.add("firstFamily".getBytes(),"name".getBytes(), Bytes.toBytes("Jon"));
        put2.add("firstFamily".getBytes(),"country".getBytes(), Bytes.toBytes("Canada"));
        put2.add("secondFamily".getBytes(),"language".getBytes(),Bytes.toBytes("French"));
        puts.add(put2);

        Put put3 = new Put("row2_1".getBytes());
        put3.add("firstFamily".getBytes(),"age".getBytes(), Bytes.toBytes(34));
        put3.add("firstFamily".getBytes(),"name".getBytes(), Bytes.toBytes("Erik"));
        put3.add("firstFamily".getBytes(),"country".getBytes(), Bytes.toBytes("Holland"));
        put3.add("secondFamily".getBytes(),"language".getBytes(),Bytes.toBytes("Dutch"));
        puts.add(put3);


        testTable.put(puts);

    }



    @Test
    public void testGetAll() throws IOException{

        AdhocQuery query = AdhocQuery.builder(testTable)
                .withColumns("firstFamily", "age")
                .withColumns("firstFamily", "name")
                .withColumns("firstFamily", "country")
                .withColumns("secondFamily", "language")
                .build();

        List<QueryResult<String, Map<String, byte[]>>> result = query.execute(String.class);

        assertEquals(3,result.size()); // result size

        // fetched columns
        assertEquals(4, result.get(0).object().keySet().size());
        assertEquals(4, result.get(1).object().keySet().size());
        assertEquals(4, result.get(2).object().keySet().size());

    }

    @Test
    public void testStartStopRowKey() throws IOException{

        AdhocQuery query = AdhocQuery.builder(testTable)
                .withColumns("firstFamily", "age")
                .withColumns("firstFamily", "name")
                .withColumns("firstFamily", "country")
                .withStartRow("row1_")
                .withStopRow("row1_~")
                .build();

        List<QueryResult<String, Map<String, byte[]>>> result = query.execute(String.class);
        assertEquals(2,result.size());

        QueryResult<String, Map<String, byte[]>> queryResult1 = result.get(0);

        assertEquals("row1_1",queryResult1.rowKey());
        assertEquals(12, Bytes.toInt(queryResult1.object().get("firstFamily.age")));
        assertEquals("Peter",Bytes.toString(queryResult1.object().get("firstFamily.name")));
        assertEquals("UK",Bytes.toString(queryResult1.object().get("firstFamily.country")));


    }

    @Test
    public void testSingleColumnFilter() throws IOException{

        AdhocQuery query = AdhocQuery.builder(testTable)
                .withColumns("firstFamily", "name")
                .withColumns("firstFamily", "age")
                .withSingleColumnValueFilter("firstFamily", "age",CompareOp.GREATER, 20)
                .build();

        List<QueryResult<String, Map<String, byte[]>>> executeResult = query.execute(String.class);
        assertEquals(2, executeResult.size());

    }

}
