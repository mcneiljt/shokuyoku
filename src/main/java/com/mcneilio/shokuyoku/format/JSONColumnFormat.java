package com.mcneilio.shokuyoku.format;

import org.json.JSONArray;
import org.json.JSONObject;

public class JSONColumnFormat {
    JSONObject original, flattened;

    public JSONColumnFormat(JSONObject original) {
        this.original = original;
        this.flattened = new JSONObject();
        flatten();
    }

    public JSONObject getFlattened() {
        return flattened;
    }

    private void flatten() {
        original.keys().forEachRemaining(key -> {
            if (original.get(key) instanceof JSONObject) {
                flatten((JSONObject) original.get(key), key);
            }
            else if (original.get(key) instanceof JSONArray) {
                flatten( (JSONArray) original.get(key), key);
            }
            else {
                String snakeCaseKey = key.replace(' ','_')
                    .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
                    .replaceAll("([a-z])([A-Z])", "$1_$2")
                    .toLowerCase();
                flattened.put(snakeCaseKey, original.get(key));
            }
        });
    }

    private void flatten(JSONObject obj, String prefix) {
        obj.keys().forEachRemaining(key -> {
            if (obj.get(key) instanceof JSONObject) {
                flatten((JSONObject) obj.get(key), prefix+"_"+key);
            }
            else if (obj.get(key) instanceof JSONArray) {
                flatten( (JSONArray) obj.get(key), prefix+"_"+key);
            }
            else {
                flattened.put(prefix+"_"+key.replace(' ','_').toLowerCase(), obj.get(key));
            }
        });
    }

    private void flatten(JSONArray array, String prefix) {
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
