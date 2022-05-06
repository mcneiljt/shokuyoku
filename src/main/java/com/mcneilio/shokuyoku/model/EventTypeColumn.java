package com.mcneilio.shokuyoku.model;

import jakarta.persistence.*;

import java.io.Serializable;
import java.sql.Timestamp;

@Entity
@Table(name = "event_type_column", indexes = {}, uniqueConstraints = {
    @UniqueConstraint(columnNames = {"event_type", "name"})})
public class EventTypeColumn {

    static public class EventTypeColumnKey implements Serializable {

        public EventTypeColumnKey() {}

        public EventTypeColumnKey(String name, String eventType) {
            this.name = name;
            this.eventType = eventType;
        }

        @Column(name = "name", nullable = false)
        private String name;

        @Column(name = "event_type",  nullable = false)
        private String eventType;
    }

    public EventTypeColumn() {}

    public EventTypeColumn(EventTypeColumnKey name, String type) {
        this.name = name;
        this.type = type;
    }

    @EmbeddedId
    private EventTypeColumnKey name;

    @Column(name = "type", nullable = false)
    private String type;

    @Column(name = "last_error")
    private Timestamp lastError;

    public EventTypeColumnKey getName() {
        return name;
    }

    public void setName(EventTypeColumnKey name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Timestamp getLastError() {
        return lastError;
    }

    public void setLastError(Timestamp lastError) {
        this.lastError = lastError;
    }
}
