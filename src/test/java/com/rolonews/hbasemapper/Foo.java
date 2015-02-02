package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.rolonews.hbasemapper.annotations.Column;
import com.rolonews.hbasemapper.annotations.Table;

import javax.annotation.Nullable;

/**
 *
 * Created by Sleiman on 13/12/2014.
 */
@Table(name = "FooTrial", columnFamilies = {"info"}, rowKeyGenerator = FooKeyGen.class)
public class Foo{

    private int id;

    @Column(family = "info",qualifier = "age")
    private int age;
    @Column(family = "info",qualifier = "name")
    private String name;

    @Column(family = "info", qualifier = "job")
    private String job;


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public Foo(){

    }

    @Override
    public String toString() {
        return "Foo{" +
                "id=" + id +
                ", age=" + age +
                ", name='" + name + '\'' +
                ", job='" + job + '\'' +
                '}';
    }
}
class FooKeyGen implements Function<Foo,String>{

    @Override
    public String apply(Foo foo) {

        return foo.getId()+"_"+foo.getName();
    }
}
