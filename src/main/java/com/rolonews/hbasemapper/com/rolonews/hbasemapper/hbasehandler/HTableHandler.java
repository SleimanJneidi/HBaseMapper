package com.rolonews.hbasemapper.com.rolonews.hbasemapper.hbasehandler;

import com.rolonews.hbasemapper.HTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 *
 * Created by Sleiman on 07/12/2014.
 *
 */
public class HTableHandler {

    private static final Lock lock = new ReentrantLock();

    private final Configuration configuration;

    public HTableHandler(final Configuration configuration){
        this.configuration = configuration;
    }

    public HTable getOrCreateHTable(HTypeInfo hTypeInfo){
        String tableName = hTypeInfo.getTable().name();
        byte[] tableNameBuffer = Bytes.toBytes(tableName);

        try {
            lock.lock();
            HBaseAdmin admin = new HBaseAdmin(configuration);
            if (!admin.isTableAvailable(tableNameBuffer)) {
                HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableNameBuffer));
                for (String family : hTypeInfo.getTable().columnFamilies()) {
                    tableDescriptor.addFamily(new HColumnDescriptor(family));
                }
                admin.createTable(tableDescriptor);
            }
            HTable hTable = new HTable(configuration, tableNameBuffer);
            return hTable;

        }catch(IOException e){
            throw new RuntimeException(e);
        }finally {
            lock.unlock();
        }
    }

    public HTable getOrCreateHTable(Class<?> clazz){
        HTypeInfo hTypeInfo = HTypeInfo.getOrRegisterHTypeInfo(clazz);
        return getOrCreateHTable(hTypeInfo);
    }

}
