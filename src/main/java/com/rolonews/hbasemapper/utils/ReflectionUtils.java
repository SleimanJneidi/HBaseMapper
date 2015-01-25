package com.rolonews.hbasemapper.utils;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

    public static Map<String,Field>  getDeclaredAndInheritedFieldsMap(final Class<?> clazz){
        final List<Field> allFields = getDeclaredAndInheritedFields(clazz);
        Map<String, Field> map = Maps.uniqueIndex(allFields, new Function<Field, String>() {

            @Override
            public String apply(Field field) {
                return field.getName();
            }

        });
        return map;
    }
}
