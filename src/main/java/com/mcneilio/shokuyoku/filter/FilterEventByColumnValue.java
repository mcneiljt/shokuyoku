package com.mcneilio.shokuyoku.filter;

import org.json.JSONObject;
import org.mortbay.util.ajax.JSON;

import java.util.ArrayList;

public class FilterEventByColumnValue {
    public FilterEventByColumnValue() {
        if (System.getenv().containsKey("EVENT_COLUMN_VALUE_FILTER")) {
            for (String filterString: System.getenv("EVENT_COLUMN_VALUE_FILTER").split("|")) {
                String[] filterComponents = filterString.split(",");
                filters.add(new ColumnValueFilter(filterComponents[0], filterComponents[1], filterComponents[2]));
            }
        }
    }

    public boolean shouldForward(JSONObject event) {
        if (filters.size() == 0) {
            return true;
        }
        else {
            for (ColumnValueFilter filter : filters) {
                switch (filter.method.toLowerCase()) {
                    case "contains":
                        return shouldForwardContains(filter.column, filter.value, event);
                    case "excludes":
                        return shouldForwardExcludes(filter.column, filter.value, event);
                    default:
                        System.out.println("UnknownFilterMethod: " + filter.method);
                }
            }
        }
        return true;
    }

    boolean shouldForwardContains(String column, String value, JSONObject event) {
        if (!event.has(column))
            return false;
        if (event.get(column) == null)
            return false;
        if (event.get(column).toString().contains(value))
            return true;
        else
            return false;
    }

    boolean shouldForwardExcludes(String column, String value, JSONObject event) {
        if (!event.has(column))
            return true;
        if (event.get(column) == null)
            return true;
        if (event.get(column).toString().contains(value))
            return false;
        else
            return true;
    }

    private ArrayList<ColumnValueFilter> filters = new ArrayList<>();

    private class ColumnValueFilter {
        ColumnValueFilter(String column, String method, String value) {
            this.column = column;
            this.method = method;
            this.value = value;
        }
        String column, method, value;
    }
}
