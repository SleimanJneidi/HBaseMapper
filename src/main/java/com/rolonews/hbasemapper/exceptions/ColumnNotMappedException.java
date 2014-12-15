package com.rolonews.hbasemapper.exceptions;

/**
 *
 * Created by Sleiman on 14/12/2014.
 */
public class ColumnNotMappedException extends RuntimeException {

    public ColumnNotMappedException(String message){
        super(message);
    }
}
