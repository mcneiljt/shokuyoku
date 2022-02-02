package com.mcneilio.shokuyoku.format;

import org.json.JSONArray;
import org.json.JSONObject;

public class JSONColumnFormat {
    JSONObject original, flattened;

    interface JSONColumnFormatFilter{
        boolean filter(String str, Object o);
    }

    public JSONColumnFormat(JSONObject original) {
        this.original = original;
        this.flattened = new JSONObject();
        flatten(null);
    }



    public JSONObject getFlattened() {
        return flattened;
    }

    private String normalizeKey(String key) {
       return key.replace(' ','_')
            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
            .replaceAll("([a-z])([A-Z])", "$1_$2")
            .toLowerCase();
    }


    private void flatten(JSONColumnFormatFilter filter) {
        flatten(original, "", filter!=null ? filter : new JSONColumnFormatFilter() {
            @Override
            public boolean filter(String str, Object o) {
                return false;
            }
        }
        );
    }

    private void flatten(JSONObject obj, String prefix, JSONColumnFormatFilter filter) {
        obj.keys().forEachRemaining(key -> {
            String normalizedKey = normalizeKey(key);
            if (obj.get(key) instanceof JSONObject) {
                // Keeping the _ at the end of prefix makes it so we can have an empty prefix
                // for the base case and also makes it so we don't have to do the extra append for the child keys.
                flatten((JSONObject) obj.get(key), prefix + normalizedKey + "_",filter);
            }
            else if (obj.get(key) instanceof JSONArray) {
                flatten( (JSONArray) obj.get(key), prefix + normalizedKey, filter);
            }
            else {
                String normalizedFullKey = prefix + normalizedKey;
                if(!filter.filter(normalizedFullKey, obj.get(key))) { // need to add the type to this filter too
                    flattened.put(normalizedFullKey, obj.get(key));
                }
            }
        });
    }

    private void flatten(JSONArray array, String prefix, JSONColumnFormatFilter filter) {
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
            flattened.put(prefix, flattenedArray);
        }
        else {
            flattened.put(prefix, array);
        }
    }
}
