package com.rolonews.hbasemapper.annotations;



import com.google.common.base.Function;

import java.lang.annotation.*;

/**
 *
 * Created by Sleiman on 06/12/2014.
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Table {

    /**
     * The name of the table.
     *
     * @return the name of the table.
     */
    String name();

    /**
     * column families' names, although we discourage having more
     * than one.
     *
     * @return column family names.
     */
    String[] columnFamilies();

    Class<? extends Function<?,?>> rowKeyGenerator();

}
