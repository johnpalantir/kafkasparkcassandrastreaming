package com.palantir.KafkaSparkCassandraStreaming.streams;

import java.io.Serializable;
import java.util.Date;

public class Person  implements Serializable {
    private Integer id;
    private String name;
    private Date birthDate;

    // Remember to declare no-args constructor
    public Person() { }

    public Person(Integer id, String name, Date birthDate) {
        this.id = id;
        this.name = name;
        this.birthDate = birthDate;
    }

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public Date getBirthDate() { return birthDate; }
    public void setBirthDate(Date birthDate) { this.birthDate = birthDate; }

    // other methods, constructors, etc.
}