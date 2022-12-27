package com.mcneilio.shokuyoku.model;

import java.util.List;

public class BatchModifierRequest {


    public static class ColumnModifier {
        private String event_type;
        private String name;
        private String type;

        public String getEvent_type() {
            return event_type;
        }

        public void setEvent_type(String event_type) {
            this.event_type = event_type;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

public BatchModifierRequest() {

}
    public BatchModifierRequest(List<ColumnModifier> modifiers) {
        this.modifiers = modifiers;
    }

    private List<ColumnModifier> modifiers;

    public List<ColumnModifier> getModifiers() {
        return modifiers;
    }

    public void setModifiers(List<ColumnModifier> modifiers) {
        this.modifiers = modifiers;
    }
}
