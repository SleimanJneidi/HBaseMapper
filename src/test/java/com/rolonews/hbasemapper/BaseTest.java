package com.rolonews.hbasemapper;

import org.apache.log4j.BasicConfigurator;
import org.junit.BeforeClass;

/**
 * Created by Sleiman on 11/12/2014.
 *
 */
public class BaseTest {

    @BeforeClass
    public static void init(){
        BasicConfigurator.configure();
    }
}
