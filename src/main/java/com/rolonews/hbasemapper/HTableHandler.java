package com.rolonews.hbasemapper;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 *
 * Created by Sleiman on 07/12/2014.
 *
 */
public class HTableHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HTableHandler.class);


    private final HConnection connection;

    public HTableHandler(final HConnection connection){
        this.connection = connection;
    }

    public HTableInterface getOrCreateHTable(EntityMapper<?> mapper){
        TableName tableName = TableName.valueOf(mapper.table().name());
        try {
            if (!connection.isTableAvailable(tableName)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for (String family : mapper.table().columnFamilies()) {
                    tableDescriptor.addFamily(new HColumnDescriptor(family));
                }

                LOG.debug(String.format("%s table does not exit, so we creating it",tableName.toString()));
                HBaseAdmin admin = new HBaseAdmin(connection.getConfiguration());
                admin.createTable(tableDescriptor);
            }
            HTableInterface table = connection.getTable(tableName);
            return table;

        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    public HTableInterface getOrCreateHTable(Class<?> clazz){
        EntityMapper<?> mapper = MappingRegistry.getMapping(clazz); //AnnotationEntityMapper.getOrRegisterAnnotationEntityMapper(clazz);

        if(mapper==null){

        }
        return getOrCreateHTable(mapper);
    }

}
