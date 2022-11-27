package com.saleskentask.studentreportingsystem.model;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.util.Date;

@Document(indexName = "students")
public class SecondSem {

    @Field(type = FieldType.Double, name = "English")
    private Double English;

    @Field(type = FieldType.Double, name = "Maths")
    private Double Maths;

    @Field(type = FieldType.Double, name = "Science")
    private Double Science;

    // Getter and Setter

    public Double getEnglish() {
        return English;
    }

    public void setEnglish(Double english) {
        English = english;
    }

    public Double getMaths() {
        return Maths;
    }

    public void setMaths(Double maths) {
        Maths = maths;
    }

    public Double getScience() {
        return Science;
    }

    public void setScience(Double science) {
        Science = science;
    }
}

