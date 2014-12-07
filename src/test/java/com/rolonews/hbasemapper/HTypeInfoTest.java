package com.rolonews.hbasemapper;

import com.rolonews.hbasemapper.annotations.*;

import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.runners.statements.ExpectException;

/**
 * Created by Sleiman on 06/12/2014.
 */
public class HTypeInfoTest {

    @Test
    public void testCanRegisterValidType(){
        HTypeInfo hTypeInfo = HTypeInfo.register(Person.class);
        Assert.assertNotNull(hTypeInfo);
    }

    @Test(expected = InvalidMappingException.class)
    public void testShouldThrowInvalidMappingException(){
        HTypeInfo.register(InvalidObject.class);
    }

    @Test
    public void testCanReadMappingFromParentClass(){
        HTypeInfo hTypeInfo = HTypeInfo.register(Student.class);
        String tableName = hTypeInfo.getTable().name();

        Assert.assertEquals("Person",tableName);
    }


}

@Table(name = "Person", rowKeys = {"id"}, columnFamilies = {"info"})
@HValidate(validator = Person.PersonValidator.class)
class Person {

    private String id;

    @Column(family = "info", qualifier = "name")
    public String name;

    @Column(family = "info", qualifier = "age")
    public int age;

    static class PersonValidator implements HEntityValidator<Person>{

        @Override
        public boolean isValid(Person person) {
            return person.name.length() > 1 ;
        }

    }
}



@HValidate(validator =Student.StudentValidator.class)
class Student extends Person{

    public class StudentValidator implements HEntityValidator<Student>{

        @Override
        public boolean isValid(Student student) {
            return student.age > 0;
        }

    }

}

@Table(name = "InvalidName", rowKeys = "id",columnFamilies = "cf")
class InvalidObject{

}



