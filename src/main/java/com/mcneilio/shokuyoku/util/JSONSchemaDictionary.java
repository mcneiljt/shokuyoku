package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.model.EventTypeColumnModifier;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.GenericDeclaration;
import java.util.*;
import java.util.function.Consumer;

import static com.mcneilio.shokuyoku.util.ShokuyokuTypes.getArrayType;

public class JSONSchemaDictionary {
    public static class EventTypeJSONSchema {
        private final JSONColumnFormat.JSONColumnFormatFilter myFilter;
        public final Set<String> prefixes;
        public final  Map<String, Set<Class>> columns;
        private final boolean ignoreNulls;

        private Map<String, EventTypeColumnModifier> modifierMap = new HashMap<>();

        public EventTypeJSONSchema(Set<String> prefixes, Map<String, Class> newColumns, boolean ignoreNulls, boolean allowInvalidCoercions, List<EventTypeColumnModifier> eventTypeColumnModifier){
            this.prefixes = prefixes;
            this.ignoreNulls = ignoreNulls;

           HashMap<String, Class> columns = new HashMap<String, Class>();
           columns.putAll(newColumns);

            if(eventTypeColumnModifier!=null){
                for (EventTypeColumnModifier eventTypeColumnModifier1 :eventTypeColumnModifier){
                    if(eventTypeColumnModifier1.getType().equals(EventTypeColumnModifier.EventColumnModifierType.DROP)){
                        columns.remove(eventTypeColumnModifier1.getName().getName());
                    } else {
                        modifierMap.put(eventTypeColumnModifier1.getName().getName(), eventTypeColumnModifier1);
                    }
                }
            }

            Map<String, Set<Class>> flattenedMap = new HashMap<>();
            columns.keySet().forEach(new Consumer<String>() {
                @Override
                public void accept(String columnName) {
                    HashSet<Class> allowedTypes = ShokuyokuTypes.getCompatibleTypes(columns.get(columnName), allowInvalidCoercions);
                    if(allowedTypes!=null){
                        flattenedMap.put(columnName, allowedTypes);
                    }
                }
            });

            this.columns = flattenedMap;

            this.myFilter = new JSONColumnFormat.JSONColumnFormatFilter() {
                private long filterCount = 0;

                @Override
                public long getFilterCount() {
                    return filterCount;
                }

                @Override
                public void resetFilterCount() {
                    filterCount = 0;
                }

                @Override
                public boolean filterPrefix(String prefix) {
                    if (!hasPrefix(prefix)) {
                        filterCount++;
                        return true;
                    }
                    return false;
                }

                @Override
                public boolean filterColumn(String str, Object o) {
                    if(!(ignoreNulls && o.equals(JSONObject.NULL)) && !hasColumn(str, o)){
                        if(!(o.equals(JSONObject.NULL)))
                            filterCount++;
                        return true;
                    }
                    return false;
                }

                @Override
                public Object modifyColumn(String str, Object o) {
                    EventTypeColumnModifier eventTypeColumnModifier = modifierMap.get(str);
                    if(eventTypeColumnModifier!=null){
                        if(eventTypeColumnModifier.getType().equals(EventTypeColumnModifier.EventColumnModifierType.DROP)){
                            return null;
                        }
                    }
                    return o;
                }
            };
        }

        public boolean hasColumn(String str, Object o) {
            Set<Class> possibleClasses = columns.get(str);
            if (possibleClasses == null)
                return false;

            if(o instanceof JSONArray){
                JSONArray jsonArray = (JSONArray) o;
                GenericDeclaration a = getArrayType(jsonArray);
                return possibleClasses.contains(a);
            } else {
                return possibleClasses.contains(o.getClass());
            }
        }

        public boolean hasPrefix(String prefix) {
            return prefixes.contains(prefix);
        }

        public JSONColumnFormat.JSONColumnFormatFilter getJSONColumnFormatFilter() {
           return myFilter;
        }
    }

    final protected Map<String, EventTypeJSONSchema> eventTypes = new HashMap<>();

    public EventTypeJSONSchema getEventJSONSchema(String eventName){
        return eventTypes.get(eventName);
    }
}
