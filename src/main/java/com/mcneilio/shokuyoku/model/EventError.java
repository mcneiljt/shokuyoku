package com.mcneilio.shokuyoku.model;

import jakarta.persistence.*;

@Entity
@Table(name = "event_error", indexes = {  @Index(
    name = "event_error_event_type_idx",
    columnList="event_type"
)}, uniqueConstraints = {
    @UniqueConstraint(columnNames = "id")})
public class EventError {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id", unique = true, nullable = false)
    private Long eventErrorId;

    @Column(name = "event_type", nullable = false)
    private String eventType;
}
