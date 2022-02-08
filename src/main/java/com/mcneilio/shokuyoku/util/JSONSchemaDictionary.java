package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;

import java.util.*;
import java.util.function.Consumer;

public class JSONSchemaDictionary {
    public static class EventTypeJSONSchema {
        public Set<String> prefixes;
        public Map<String, Set<Class>> columns;

        public EventTypeJSONSchema(Set<String> prefixes, Map<String, Class> columns){
            this.prefixes = prefixes;

            Map<String, Set<Class>> flattenedMap = new HashMap<>();
            columns.keySet().forEach(new Consumer<String>() {
                @Override
                public void accept(String columnName) {
                    Class type = columns.get(columnName);
                    if (type.equals(Double.class)){
                        flattenedMap.put(columnName, new HashSet(Arrays.asList(new Class[] {Integer.class, Double.class})));

                    } else if(type.equals(Boolean.class)){
                        flattenedMap.put(columnName, new HashSet(Arrays.asList(new Class[] {Boolean.class})));

                    } else if(type.equals(String.class)){
                        flattenedMap.put(columnName, new HashSet(Arrays.asList(new Class[] {String.class, Integer.class, Double.class, Boolean.class})));
                    }
                }
            });

            this.columns = flattenedMap;
        }

        public boolean hasColumn(String str, Object o) {
            Set<Class> possibleClasses = columns.get(str);
            if (possibleClasses == null)
                return false;

            return possibleClasses.contains(o.getClass());
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