package com.rolonews.hbasemapper;

import com.google.common.base.Stopwatch;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.slf4j.Logger;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Sleiman on 11/12/2014.
 *
 */
public class BaseTest {

    @BeforeClass
    public static void init(){
        BasicConfigurator.configure();
        org.apache.log4j.Logger.getRootLogger().setLevel(Level.ERROR);
    }


    protected static MiniHBaseCluster miniHBaseCluster;

    protected static HConnection getMiniClusterConnection(){
        if(miniHBaseCluster == null){
            try {
                HBaseTestingUtility hBaseTestingUtility = new HBaseTestingUtility();
                miniHBaseCluster = hBaseTestingUtility.startMiniCluster();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        try {
            return HConnectionManager.createConnection(miniHBaseCluster.getConfiguration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    int rowCount(HTableInterface table,String family){
        int count=0;
        ResultScanner scanner;
        try {
            scanner = table.getScanner(Bytes.toBytes(family));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (Result result : scanner) {
            count++;
        }
        return count;
    }
}
