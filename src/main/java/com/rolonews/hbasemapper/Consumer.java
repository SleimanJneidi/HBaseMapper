package com.rolonews.hbasemapper;

/**
 *
 * Created by Sleiman on 13/12/2014.
 *
 */

public interface Consumer<T> {
    void consume(T t);
}
