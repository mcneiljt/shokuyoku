package com.mcneilio.shokuyoku.format;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.function.Function;

public class JSONColumnFormat {
    JSONObject original, flattened;

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


    private void flatten(Function<String, String> filter) {
        flatten(original, "", filter);
    }

    private void flatten(JSONObject obj, String prefix, Function<String, String> filter) {
        obj.keys().forEachRemaining(key -> {
            String normalizedKey = normalizeKey(key);
            if (obj.get(key) instanceof JSONObject) {
                // Keeping the _ at the end of prefix makes it so we can have an empty prefix
                // for the base case and also makes it so we don't have to do the extra append for the child keys.
                flatten((JSONObject) obj.get(key), prefix+normalizedKey+"_",filter);
            }
            else if (obj.get(key) instanceof JSONArray) {
                flatten( (JSONArray) obj.get(key), prefix+normalizedKey, filter);
            }
            else {
                flattened.put(prefix+normalizedKey, obj.get(key));
            }
        });
    }

    private void flatten(JSONArray array, String prefix, Function<String, String> filter) {
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
                flattenedArray.put(v.toString());
            }
            flattened.put(prefix, flattenedArray);
        }
        else {
            flattened.put(prefix, array);
        }
    }
}
