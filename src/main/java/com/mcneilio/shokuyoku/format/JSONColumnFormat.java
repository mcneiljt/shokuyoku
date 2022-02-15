package com.mcneilio.shokuyoku.format;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

public class JSONColumnFormat {
    JSONObject original;

    public interface JSONColumnFormatFilter {

        long getFilterCount();

        void resetFilterCount();

        boolean filterPrefix(String prefix);

        boolean filterColumn(String str, Object o);
    }

    public JSONColumnFormat(JSONObject original) {
        this.original = original;
    }

    public JSONObject getFlattened() {
        return getCopy(null, true);
    }

    public JSONObject getCopy(JSONColumnFormatFilter filter, boolean shouldFlatten, Set<String> hoistFields) {
        return flatten(filter, shouldFlatten, hoistFields);
    }
    public JSONObject getCopy(JSONColumnFormatFilter filter, boolean shouldFlatten) {
        return flatten(filter, shouldFlatten, Collections.EMPTY_SET);
    }

    public static String normalizeKey(String key) {
        return key.replace(' ', '_')
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
            .replaceAll("([a-z])([A-Z])", "$1_$2")
            .toLowerCase();
    }

    private JSONObject flatten(JSONColumnFormatFilter filter, boolean shouldFlatten, Set<String> hoistFields) {
        JSONObject flattened = new JSONObject();
        flatten(flattened, original, "", filter != null ? filter : new JSONColumnFormatFilter() {
            @Override
            public long getFilterCount() {
                return 0;
            }

            @Override
            public void resetFilterCount() {
            }

            @Override
                public boolean filterPrefix(String str) {
                    return false;
                }

                @Override
                public boolean filterColumn(String str, Object o) {
                    return false;
                }
            }, shouldFlatten, hoistFields
        );
        return flattened;
    }

    private void flatten(JSONObject dest, JSONObject obj, String prefix, JSONColumnFormatFilter filter, boolean shouldFlatten, Set<String> hoistFields) {
        obj.keys().forEachRemaining(key -> {
            String normalizedKey = normalizeKey(key);
            if (obj.get(key) instanceof JSONObject) {
                if(hoistFields.contains(key)){
                    return;
                }
                // Keeping the _ at the end of prefix makes it so we can have an empty prefix
                // for the base case and also makes it so we don't have to do the extra append for the child keys.
                if (!filter.filterPrefix(prefix + normalizedKey)) {
                    if (shouldFlatten) {
                        flatten(dest, (JSONObject) obj.get(key), prefix + normalizedKey + "_", filter, shouldFlatten, Collections.EMPTY_SET);
                    } else{
                        JSONObject newValue = new JSONObject();
                        dest.put(key,newValue);
                        flatten(newValue, (JSONObject) obj.get(key), prefix + normalizedKey + "_", filter, shouldFlatten, Collections.EMPTY_SET);
                    }
                }
            } else if (obj.get(key) instanceof JSONArray) {
                if (!filter.filterColumn(prefix + normalizedKey, obj.get(key))) { // need to add the type to this filter too
                    JSONArray arr =flatten(dest, (JSONArray) obj.get(key), prefix + normalizedKey, filter, shouldFlatten);
                    dest.put(prefix + normalizedKey, obj.get(key));
                }
            } else {
                String normalizedFullKey = prefix + normalizedKey;
                if (!filter.filterColumn(normalizedFullKey, obj.get(key))) { // need to add the type to this filter too

                    dest.put(shouldFlatten ? normalizedFullKey : key, obj.get(key));
                }
            }
        });

        hoistFields.forEach(key -> {
       //     String normalizedKey = normalizeKey(key);

          //  if (shouldFlatten) {
                flatten(dest, (JSONObject) obj.get(key), "", filter, shouldFlatten, Collections.EMPTY_SET);
//            } else{
//                JSONObject newValue = new JSONObject();
//                dest.put(key,newValue);
//                flatten(newValue, (JSONObject) obj.get(key), prefix + normalizedKey + "_", filter, shouldFlatten, Collections.EMPTY_SET);
//            }
        });
    }

    private JSONArray flatten(JSONObject dest, JSONArray array, String prefix, JSONColumnFormatFilter filter, boolean shouldFlatten) {
        boolean complexType = false;
        for (Object v : array.toList()) {
            if (v instanceof JSONObject || v instanceof JSONArray) {
                complexType = true;
                break;
            }
        }
        if (complexType) {
            JSONArray flattenedArray = new JSONArray();
            for (Object v : array.toList()) {
                // TODO: Should we flatten these objects?
                flattenedArray.put(v.toString());
            }
            return flattenedArray;
        } else {
            return array;
        }
    }

    public static void main(String[] args) {
        JSONColumnFormat obj = new JSONColumnFormat(new JSONObject("{}"));
    }
}
