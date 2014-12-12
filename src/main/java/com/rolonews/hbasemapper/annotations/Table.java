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
public @interface Table {

    /**
     * The name of the table.
     *
     * @return the name of the table.
     */
    String name();

    /**
     * fields that represent the row key of the table.
     * It is very common that you have a composite key
     * composed from different fields, so you can provide
     * these field names as an array of strings.
     *
     * @return the name of fields that represent the row key.
     */
    String[] rowKey() default {};

    /**
     * a string literal that separates the fields that compose
     * the row key if it's composite.
     *
     * @return row-key separator.
     */
    String rowKeySeparator() default "_";

    /**
     * column families' names, although we discourage having more
     * than one.
     *
     * @return column family names.
     */
    String[] columnFamilies();

}
