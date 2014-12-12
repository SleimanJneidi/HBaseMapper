package com.rolonews.hbasemapper;

import com.rolonews.hbasemapper.annotations.*;

import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import static org.junit.Assert.*;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Created by Sleiman on 06/12/2014.
 */
public class HTypeInfoTest extends BaseTest {

    @Test
    public void testCanRegisterValidType(){
        HTypeInfo hTypeInfo = HTypeInfo.register(Person.class);
        assertNotNull(hTypeInfo);
    }

    @Test(expected = InvalidMappingException.class)
    public void testShouldThrowInvalidMappingException(){
        HTypeInfo.register(InvalidObject.class);
    }

    @Test
    public void testCanReadMappingFromParentClass(){
        HTypeInfo hTypeInfo = HTypeInfo.register(Student.class);
        String tableName = hTypeInfo.getTable().name();

        assertEquals("Person", tableName);
    }

    @Test
    public void testCanValidateEntity() throws IllegalAccessException, InstantiationException, InvocationTargetException {

        Student student = new Student();
        student.age = 14;
        student.name = "Peter";

        HTypeInfo hTypeInfo = HTypeInfo.register(student.getClass());
        List<HValidate> validators = hTypeInfo.getValidators();

        for(HValidate validator: validators){
            Class<? extends HEntityValidator<?>> entityValidator = validator.validator();

            HEntityValidator<Student> hEntityValidator = (HEntityValidator<Student>)entityValidator.newInstance();
            assertTrue(hEntityValidator.isValid(student));
        }
    }

    @Test
    public void testCanMapInheritedClass(){
        HTypeInfo typeInfo = HTypeInfo.getOrRegisterHTypeInfo(SubInheritanceDummy.class);
        assertEquals("BaseInheritanceDummy",typeInfo.getTable().name());
    }

}

@Table(name = "Person", rowKey = {"id"}, columnFamilies = {"info"})
@HValidate(validator = PersonValidator.class)
class Person {

    private String id;

    @Column(family = "info", qualifier = "name")
    public String name;

    @Column(family = "info", qualifier = "age")
    public int age;


}

class PersonValidator implements HEntityValidator<Person>{

    @Override
    public boolean isValid(Person person) {
        return person.name.length() > 1 ;
    }

}

@HValidate(validator =StudentValidator.class)
class Student extends Person{



}

class StudentValidator implements HEntityValidator<Student>{

    @Override
    public boolean isValid(Student student) {
        return student.age > 0;
    }

}
@Table(name = "InvalidName", rowKey = "id",columnFamilies = "cf")
class InvalidObject{

}

@Table(name = "BaseInheritanceDummy", rowKey = "id",columnFamilies = "details")
class BaseInheritanceDummy{
    private String id;
}
class SubInheritanceDummy extends BaseInheritanceDummy{

}



