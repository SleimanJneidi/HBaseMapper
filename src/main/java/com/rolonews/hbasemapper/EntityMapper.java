package com.rolonews.hbasemapper;

import com.rolonews.hbasemapper.annotations.Column;
import com.rolonews.hbasemapper.annotations.Table;

import java.lang.reflect.Field;
import java.util.Map;

/**
 * Created by Sleiman on 25/01/2015.
 */
interface EntityMapper<T>{
    Class<T> clazz();
    Table table();
    Map<String,Field> rowKeys();
    Map<Column,Field> columns();
}