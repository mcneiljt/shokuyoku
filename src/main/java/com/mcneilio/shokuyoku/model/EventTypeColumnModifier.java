package com.mcneilio.shokuyoku.model;

import jakarta.persistence.*;

import java.io.Serializable;
import java.sql.Timestamp;

@Entity
@Table(name = "event_type_column_modifier", indexes = {   @Index(
    name = "event_type_column_modifier_name_idx",
    columnList="name"
) }, uniqueConstraints = {
    @UniqueConstraint(columnNames = {"name"})})
public class EventTypeColumnModifier {

    public EventTypeColumnModifier() {}

    public EventTypeColumnModifier(EventTypeColumn.EventTypeColumnKey name, EventColumnModifierType type, Timestamp createdAt) {
        this.name = name;
        this.type = type;
        this.createdAt = createdAt;
    }

    @EmbeddedId
    private EventTypeColumn.EventTypeColumnKey name;

    public enum EventColumnModifierType {
        DROP
    };

    @Column(name = "modifier",  nullable = false)
    private EventColumnModifierType type;

    @Column(name = "created_at")
    private Timestamp createdAt;

    public EventTypeColumn.EventTypeColumnKey getName() {
        return name;
    }

    public void setName(EventTypeColumn.EventTypeColumnKey name) {
        this.name = name;
    }

    public EventColumnModifierType getType() {
        return type;
    }

    public void setType(EventColumnModifierType type) {
        this.type = type;
    }

    public Timestamp getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Timestamp createdAt) {
        this.createdAt = createdAt;
    }
}
