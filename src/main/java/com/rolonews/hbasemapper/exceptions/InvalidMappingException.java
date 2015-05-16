package com.rolonews.hbasemapper.exceptions;

/**
 *
 * Created by Sleiman on 06/12/2014.
 *
 */
public class InvalidMappingException extends RuntimeException{

	private static final long serialVersionUID = 1L;

	public InvalidMappingException(String message){
        super(message);
    }

}
