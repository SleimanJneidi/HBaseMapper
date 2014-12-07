package com.rolonews.hbasemapper;

/**
 * Created by Sleiman on 06/12/2014.
 */
/**
 *
 * @author Sleiman
 * @param <T>
 */
public interface HEntityValidator<T> {
    boolean isValid(T object);
}
