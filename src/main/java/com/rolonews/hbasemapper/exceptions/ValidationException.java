package com.rolonews.hbasemapper.exceptions;

import com.google.common.base.Predicate;

/**
 * Created by Sleiman on 16/04/2015.
 */

public class ValidationException extends RuntimeException {

    public ValidationException(Predicate<?> predicate){
        super("Object is not valid, you violating the validator rule "+predicate.getClass());
    }
}
