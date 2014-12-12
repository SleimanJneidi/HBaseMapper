package com.rolonews.hbasemapper;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.log4j.BasicConfigurator;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 * Created by Sleiman on 11/12/2014.
 *
 */
public class BaseTest {

    @BeforeClass
    public static void init(){
        BasicConfigurator.configure();
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
}
