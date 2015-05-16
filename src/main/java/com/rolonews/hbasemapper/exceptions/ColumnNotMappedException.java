package com.rolonews.hbasemapper.exceptions;

/**
 *
 * Created by Sleiman on 14/12/2014.
 */
public class ColumnNotMappedException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public ColumnNotMappedException(String message){
        super(message);
    }
}
