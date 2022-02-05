package com.mcneilio.shokuyoku.util;

public class SimpleJSONSchemaDictionary extends JSONSchemaDictionary{

    public void addEventType(String eventType, EventTypeJSONSchema eventTypeJSONSchema){
        this.eventTypes.put(eventType, eventTypeJSONSchema);
    }
}
