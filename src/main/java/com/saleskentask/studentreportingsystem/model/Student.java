package com.saleskentask.studentreportingsystem.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "students", createIndex = true)
public class Student {

    @Id
    private String id;

    @Field(type = FieldType.Text, name = "name")
    private String name;

    @Field(type = FieldType.Nested, name = "firstSem")
    private FirstSem firstSem;

    @Field(type = FieldType.Nested, name = "secondSem")
    private SecondSem secondSem;


    // Getter and Setter

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public FirstSem getFirstSem() {
        return firstSem;
    }

    public void setFirstSem(FirstSem firstSem) {
        this.firstSem = firstSem;
    }

    public SecondSem getSecondSem() {
        return secondSem;
    }

    public void setSecondSem(SecondSem secondSem) {
        this.secondSem = secondSem;
    }
}
