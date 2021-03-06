package com.rolonews.hbasemapper.mapping;

import com.google.common.base.Preconditions;
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
        Preconditions.checkNotNull(mapper);

        TableName tableName = mapper.tableDescriptor().getTableName();
        try {
            if (!connection.isTableAvailable(tableName)) {

                LOG.debug("{} table does not exit, so we creating it", tableName.getNameAsString());
                HBaseAdmin admin = new HBaseAdmin(connection.getConfiguration());
                admin.createTable(mapper.tableDescriptor());
                admin.close();
            }
            HTableInterface table = connection.getTable(tableName);
            return table;

        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    public HTableInterface getOrCreateHTable(Class<?> clazz){
        EntityMapper<?> mapper = MappingRegistry.getMapping(clazz);
        return getOrCreateHTable(mapper);
    }

}
