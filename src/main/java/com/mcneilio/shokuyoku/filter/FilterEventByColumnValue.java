package com.mcneilio.shokuyoku.filter;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;

public class FilterEventByColumnValue {
    public FilterEventByColumnValue(boolean isEventColumnContainsFilterEnabled, boolean isEventColumnExcludesFilterEnabled) {
        if (System.getenv().containsKey("EVENT_COLUMN_VALUE_FILTER")) {
            String[] filterConfig = System.getenv("EVENT_COLUMN_VALUE_FILTER").split("\\|");

            for (String filterString: filterConfig) {
                System.out.println("event column filter: " + filterString);
                String[] filterComponents = filterString.split(",");

                if (filterComponents.length != 4){
                    // if required value not provided, exit
                    System.out.println("Invalid EVENT_COLUMN_VALUE_FILTER value: " + filterString + ". Value must be event_name, column, method, value");
                    System.exit(1);
                }

                // add the filter only if the filter's method is enabled
                if ((filterComponents[2].equalsIgnoreCase("CONTAINS") && isEventColumnContainsFilterEnabled) ||
                    (filterComponents[2].equalsIgnoreCase("EXCLUDES") && isEventColumnExcludesFilterEnabled)) {
                    filters.add(new ColumnValueFilter(filterComponents[0], filterComponents[1], filterComponents[2], filterComponents[3]));

                    if (!filterEventNames.contains(filterComponents[0]))
                        filterEventNames.add(filterComponents[0]);

                    if (filterComponents[0].equalsIgnoreCase("ALL"))
                        hasAllEvent = true;
                }
            }
        }
    }

    public boolean shouldForward(JSONObject event) {

        // if no filters defined, return true
        if (filters.size() == 0)
            return true;

        try {
            // if there are no ALL event filter and the event name does not have a defined filter, return true
            if (!hasAllEvent && !filterEventNames.contains(event.getString("event")))
                return true;
        }
        catch(JSONException ex) {
            // the event name is not included in the event, so do not check the filters
            return true;
        }

        boolean hasContainsFilter = false;

        for (ColumnValueFilter filter : filters) {
            switch (filter.method.toLowerCase()) {
                case "contains":
                    // if any filter matches on contains, return true
                    if (shouldForwardContains(filter.eventName, filter.column, filter.value, event))
                        return true;

                    if (filter.eventName.equalsIgnoreCase(event.getString("event")))
                        hasContainsFilter = true;

                    break;
                case "excludes":
                     // if any filter matches on exclusion, return false
                     if (!shouldForwardExcludes(filter.eventName, filter.column, filter.value, event))
                         return false;

                     break;
                default:
                    System.out.println("UnknownFilterMethod: " + filter.method);
            }
        }

        // if we have a contains filter for the event name and no contains matches were found, return false
        if (hasContainsFilter)
            return false;

        // if we get here, there is no contains filter for the event name and no exclusion filter matches were found, so return true
        return true;
    }

    boolean shouldForwardContains(String eventName, String column, String value, JSONObject event) {

        // if the event name is "ALL" OR (event name is specified AND matches)
        if ((eventName.equalsIgnoreCase("ALL") ||
            (event.get("event") != null && event.get("event").toString().equalsIgnoreCase(eventName)))) {

            String columnValue = getColumnValue(event, column);

            if (columnValue != null) {
                System.out.println(eventName + " does not have column " + column + " or the value is null");
                return false;
            }

            // Return true if the contains filter matches, otherwise false
            return columnValue.contains(value);
        }

        // if we get here, there was no matching contains filter for the event name, so return true
        return true;
    }

    boolean shouldForwardExcludes(String eventName, String column, String value, JSONObject event) {

        // if the event name is "ALL" OR (the event name is specified and matches)
        if (eventName.equalsIgnoreCase("ALL") ||
            (event.get("event") != null && event.get("event").toString().equalsIgnoreCase(eventName))) {

            String columnValue = getColumnValue(event, column);

            if (columnValue == null) {
                System.out.println(eventName + " does not have column " + column + " or the value is null");
                return true;
            }

            // Return false if the exlusion filter matches, otherwise true
            return !columnValue.equalsIgnoreCase(value);
        }

        // if we get here, there was no matching excludes filter for the event name, so return true
        return true;
    }

    private String getColumnValue(JSONObject event, String column) {
        String[] columns = column.split("\\.");

        JSONObject json = event;

        for (int i = 0; i < columns.length; i++){

            if (i == (columns.length - 1))
                return json.getString(columns[i]);

            json = json.getJSONObject(columns[i]);
        }

        return null;
    }

    private final ArrayList<ColumnValueFilter> filters = new ArrayList<>();
    private final ArrayList<String> filterEventNames = new ArrayList<>();

    private boolean hasAllEvent = false;

    private class ColumnValueFilter {
        ColumnValueFilter(String eventName, String column, String method, String value) {
            this.eventName = eventName;
            this.column = column;
            this.method = method;
            this.value = value;
        }
        String column, method, value, eventName;
    }
}
