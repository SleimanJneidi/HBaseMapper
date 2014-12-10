package com.rolonews.hbasemapper.annotations;


import java.lang.annotation.*;

/**
 *
 * Created by Sleiman on 06/12/2014.
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface MTable {

    String name();
    String[] rowKeys();
    String rowKeySeperator() default "_";
    String[] columnFamilies();

}
