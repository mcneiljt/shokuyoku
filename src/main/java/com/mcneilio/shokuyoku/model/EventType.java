package com.mcneilio.shokuyoku.model;

import jakarta.persistence.*;

import java.sql.Timestamp;

@Entity
@Table(name = "event_type", indexes = {   @Index(
    name = "event_type_name_idx",
    columnList="name"
) }, uniqueConstraints = {
    @UniqueConstraint(columnNames = {"name"})})
public class EventType {


    public EventType() {   super();}

    public EventType(String name, Timestamp lastError, Boolean exists){
        super();
        this.name = name;
        this.lastError = lastError;
        this.hive_exists = exists;
    }

    @Id
    @Column(name = "name", unique = true, nullable = false)
    private String name;

    @Column(name = "last_error")
    private Timestamp lastError;

    @Column(name = "hive_exists", nullable = false)
    private Boolean hive_exists;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Timestamp getLastError() {
        return lastError;
    }

    public void setLastError(Timestamp lastError) {
        this.lastError = lastError;
    }

    public Boolean getHive_exists() {
        return hive_exists;
    }

    public void setHive_exists(Boolean hive_exists) {
        this.hive_exists = hive_exists;
    }
}
