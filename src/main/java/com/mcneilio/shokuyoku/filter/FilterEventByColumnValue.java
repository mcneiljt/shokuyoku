package com.mcneilio.shokuyoku.filter;

import org.json.JSONObject;

import java.util.ArrayList;

public class FilterEventByColumnValue {
    public FilterEventByColumnValue(JSONObject event) {
        this.event = event;
        if (System.getenv().containsKey("EVENT_COLUMN_VALUE_FILTER")) {
            for (String filterString: System.getenv("EVENT_COLUMN_VALUE_FILTER").split("|")) {
                filters.add(new ColumnValueFilter(filterString.split(",")));
            }
        }
    }

    public boolean shouldForward() {
        if (filters.size() == 0) {
            return true;
        }
        else {
            for (ColumnValueFilter filter : filters) {
                switch (filter.method.toLowerCase()) {
                    case "contains":
                        return shouldForwardContains(filter.column, filter.value);
                    case "excludes":
                        return shouldForwardExcludes(filter.column, filter.value);
                    default:
                        System.out.println("UnknownFilterMethod: " + filter.method);
                }
            }
        }
        return true;
    }

    boolean shouldForwardContains(String column, String value) {
        if (!event.has(column))
            return false;
        if (event.get(column) == null)
            return false;
        if (event.get(column).toString().contains(value))
            return true;
        else
            return false;
    }

    boolean shouldForwardExcludes(String column, String value) {
        if (!event.has(column))
            return true;
        if (event.get(column) == null)
            return true;
        if (event.get(column).toString().contains(value))
            return false;
        else
            return true;
    }

    private JSONObject event;
    private ArrayList<ColumnValueFilter> filters = new ArrayList<>();

    private class ColumnValueFilter {
        ColumnValueFilter(String[] filterComponents) {
            this.column = filterComponents[0];
            this.method = filterComponents[1];
            this.value = filterComponents[2];
        }
        String column, method, value;
    }
}
