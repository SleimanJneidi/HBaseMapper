package com.rolonews.hbasemapper.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * Created by Sleiman on 15/12/2014.
 */
public class ReflectionUtils {

    public static List<Field> getDeclaredAndInheritedFields(final Class<?> clazz) {
        final List<Field> allFields = new ArrayList<Field>();
        allFields.addAll(Arrays.asList(clazz.getDeclaredFields()));

        Class<?> parent = clazz.getSuperclass();
        while ((parent != null) && (parent != Object.class)) {
            allFields.addAll(Arrays.asList(parent.getDeclaredFields()));
            parent = parent.getSuperclass();
        }

        return allFields;
    }
}
