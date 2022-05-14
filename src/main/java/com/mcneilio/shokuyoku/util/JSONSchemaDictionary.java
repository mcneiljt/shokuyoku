package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.lang.reflect.GenericDeclaration;
import java.util.*;
import java.util.function.Consumer;

import static com.mcneilio.shokuyoku.util.ShokuyokuTypes.getArrayType;

public class JSONSchemaDictionary {
    public static class EventTypeJSONSchema {
        public Set<String> prefixes;
        public Map<String, Set<Class>> columns;
        private boolean ignoreNulls;

        public EventTypeJSONSchema(Set<String> prefixes, Map<String, Class> columns, boolean ignoreNulls, boolean allowInvalidCoercions){
            this.prefixes = prefixes;
            this.ignoreNulls = ignoreNulls;

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
        }

        public boolean hasColumn(String str, Object o) {
            Set<Class> possibleClasses = columns.get(str);
            if (possibleClasses == null)
                return false;

            if(o instanceof JSONArray){
                JSONArray jsonArray = (JSONArray) o;
                GenericDeclaration a = getArrayType(jsonArray);
                return possibleClasses.contains(a);

//                if(jsonArray.length()==0){
//                    return possibleClasses.contains(Array.class);
//                }
//                Class c = jsonArray.get(0).getClass();
//                for(int i=1;i<jsonArray.length();i++){
//                    if(c.equals(String.class) || c.equals(JSONObject.class)){
//                        break;
//                    }
//                    Class newC = jsonArray.get(i).getClass();
//                    if(!c.equals(newC)){
//                        return possibleClasses.contains(String.class);
//                    }
//                }
//
//                if(c.equals(JSONObject.class)) {
//                    return possibleClasses.contains(String.class);
//                }
//
//                return possibleClasses.contains(Array.newInstance(c, 0).getClass());
            } else {
                return possibleClasses.contains(o.getClass());
            }
        }

        public boolean hasPrefix(String prefix) {
            return prefixes.contains(prefix);
        }

        public JSONColumnFormat.JSONColumnFormatFilter getJSONColumnFormatFilter() {
            return new JSONColumnFormat.JSONColumnFormatFilter() {
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
            };
        }
    }

    protected Map<String, EventTypeJSONSchema> eventTypes = new HashMap<>();

    public EventTypeJSONSchema getEventJSONSchema(String eventName){
        return eventTypes.get(eventName);
    }
}
