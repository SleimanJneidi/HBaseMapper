package com.rolonews.hbasemapper.annotations;

import java.lang.annotation.*;

/**
 *
 * @author Sleiman
 *
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface MColumn {
    String family();
    String qualifier();
}
