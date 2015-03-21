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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.rolonews.hbasemapper.BaseTest;

public class RowGetterTest extends BaseTest{

    private static HTableInterface testTable;

    @BeforeClass
    public static void setup() throws IOException{

        HConnection connection = BaseTest.getMiniClusterConnection();
        HBaseAdmin admin = new HBaseAdmin(connection);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("RowGetterTableTest"));

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
        testTable = connection.getTable("RowGetterTableTest");

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
    public void testGetRowById() throws IOException{
        RowGetter rowGetter = RowGetter.projector(testTable)
                .withColumns("firstFamily", "age")
                .withColumns("firstFamily", "name")
                .build();

        Optional<Map<String, byte[]>> optionalResult = rowGetter.get("row1_1");

        assertTrue(optionalResult.isPresent());
        Map<String, byte[]> resultMap = optionalResult.get();

        assertEquals(2, resultMap.size());
        assertEquals(12, Bytes.toInt(resultMap.get("firstFamily.age")));
        assertEquals("Peter", Bytes.toString(resultMap.get("firstFamily.name")));

    }





}
