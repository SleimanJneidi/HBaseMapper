package com.rolonews.hbasemapper.annotations;

import com.rolonews.hbasemapper.HEntityValidator;
import java.lang.annotation.*;

/**
 *
 * Created by Sleiman on 06/12/2014.
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface HValidate {
    Class<? extends HEntityValidator<?>> validator();
}
