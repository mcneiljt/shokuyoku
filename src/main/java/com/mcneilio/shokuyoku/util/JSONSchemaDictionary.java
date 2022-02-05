package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class JSONSchemaDictionary {
    public static class EventTypeJSONSchema {
        public Set<String> prefixes;
        public Map<String, Class> columns;

        public EventTypeJSONSchema(Set<String> prefixes, Map<String, Class> columns){
            this.prefixes = prefixes;
            this.columns = columns;
        }

        public boolean hasColumn(String str, Object o) {
            Class c = columns.get(str);
            if(c==null)
                return false;

            // TODO check inheritance.
            return c.equals(o.getClass());
        }

        public boolean hasPrefix(String prefix) {
            return prefixes.contains(prefix);
        }

        public JSONColumnFormat.JSONColumnFormatFilter getJSONColumnFormatFilter() {
            return new JSONColumnFormat.JSONColumnFormatFilter() {
                @Override
                public boolean filterPrefix(String prefix) {
                    return !hasPrefix(prefix);
                }

                @Override
                public boolean filterColumn(String str, Object o) {
                    return !hasColumn(str, o);
                }
            };
        }
    }

    protected Map<String, EventTypeJSONSchema> eventTypes = new HashMap<>();

    public EventTypeJSONSchema getEventJSONSchema(String eventName){
        return eventTypes.get(eventName);
    }
}
