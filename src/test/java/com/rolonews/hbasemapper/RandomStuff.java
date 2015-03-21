package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.rolonews.hbasemapper.query.Query;
import com.rolonews.hbasemapper.query.QueryResult;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.io.TestGenericWritable;

import java.lang.reflect.Type;

/**
 * Created by Sleiman on 08/12/2014.
 *
 */
public class RandomStuff {


    public static void main(String[] args) throws Exception {

        RandomStuff.fn("s", 1);
    }

    /*
    static <T> void fn(){
        TypeToken<T> token = new TypeToken<T>() {
        };
        Class<? super T> rawType = token.getRawType();

        //Type mySuperclass = token.getClass().getGenericSuperclass();
        System.out.println(rawType.getName());
    }
    */
    static <K,T> void fn(K k,T t){
        /*
        TypeToken<QueryResult<K,T>> token = new TypeToken<QueryResult<K,T>>() {
        };
        */
        com.google.common.reflect.TypeToken<QueryResult<K,T>> of = new com.google.common.reflect.TypeToken<QueryResult<K, T>>() {
        };
        com.google.common.reflect.TypeToken<?> typeToken = of.resolveType(QueryResult.class.getTypeParameters()[0]);
        System.out.println(typeToken.getType());

        com.google.common.reflect.TypeToken<?> typeToken1 = of.resolveType(QueryResult.class.getTypeParameters()[1]);
        System.out.println(typeToken1.getType());

    }




}






