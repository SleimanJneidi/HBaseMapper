package com.rolonews.hbasemapper;

import com.google.common.base.Function;
import com.rolonews.hbasemapper.annotations.*;

import com.rolonews.hbasemapper.exceptions.InvalidMappingException;
import static org.junit.Assert.*;
import org.junit.Test;

import javax.annotation.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Created by Sleiman on 06/12/2014.
 */
public class AnnotationMapperTest extends BaseTest {

    @Test
    public void testCanRegisterValidType(){
        EntityMapper<Person> hTypeInfo = AnnotationEntityMapper.createAnnotationMapping(Person.class);
        assertNotNull(hTypeInfo);
    }

    @Test
    public void testCanReadMappingFromParentClass(){
        EntityMapper<?> hTypeInfo = AnnotationEntityMapper.createAnnotationMapping(Student.class);
        String tableName = hTypeInfo.tableDescriptor().getTableName().getNameAsString();

        assertEquals("Person", tableName);
    }


    @Test
    public void testCanMapInheritedClass(){
        EntityMapper<?> typeInfo = AnnotationEntityMapper.createAnnotationMapping(SubInheritanceDummy.class);
        assertEquals("BaseInheritanceDummy",typeInfo.tableDescriptor().getTableName().getNameAsString());
    }

}

@Table(name = "Person", columnFamilies = {"info"}, rowKeyGenerator = PersonKey.class)
@HValidate(validator = PersonValidator.class)
class Person {

    public String id;

    @Column(family = "info", qualifier = "name")
    public String name;

    @Column(family = "info", qualifier = "age")
    public int age;


}

class PersonKey implements Function<Person,String>{

    @Nullable
    @Override
    public String apply(Person input) {
        return input.id;
    }
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


@Table(name = "BaseInheritanceDummy", rowKeyGenerator = FooKeyGen.class ,columnFamilies = "details")
class BaseInheritanceDummy{
    private String id;
}
class SubInheritanceDummy extends BaseInheritanceDummy{

}



