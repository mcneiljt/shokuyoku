package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.model.EventTypeColumnModifier;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OrcJSONSchemaDictionary extends JSONSchemaDictionary {

    public  OrcJSONSchemaDictionary(String hiveURL, String databaseName, boolean ignoreNulls, boolean allowInvalidCoercions, HashMap<String, HashMap<String, Class>> schemaOverrides) {
        this(hiveURL, databaseName, ignoreNulls, allowInvalidCoercions, schemaOverrides, null);
    }
    public  OrcJSONSchemaDictionary(String hiveURL, String databaseName, boolean ignoreNulls, boolean allowInvalidCoercions, HashMap<String, HashMap<String, Class>> schemaOverrides, List<EventTypeColumnModifier> eventTypeColumnModifierList){

        boolean useCache = System.getenv("CACHE_ENABLED") != null &&
            System.getenv("CACHE_ENABLED").equalsIgnoreCase("TRUE");

        Map<String, List<EventTypeColumnModifier>> columnModifierMap = new HashMap<>();
        if (eventTypeColumnModifierList!=null){
            for(EventTypeColumnModifier eventTypeColumnModifier: eventTypeColumnModifierList){
                if(!columnModifierMap.containsKey(eventTypeColumnModifier.getName().getEventType()))
                    columnModifierMap.put(eventTypeColumnModifier.getName().getEventType(), new ArrayList<>());
                columnModifierMap.get(eventTypeColumnModifier.getName().getEventType()).add(eventTypeColumnModifier);
            }
        }

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveURL);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        long startTime = System.currentTimeMillis();

        OrcJSONSchemaDictionaryCache cache = null;

        try {
            List<String> tableNames;
            final String schema = databaseName;

            if (useCache) {
                System.out.println("Loading event names from cache");
                cache = new OrcJSONSchemaDictionaryCache();
                tableNames = cache.getEventNameList();

                if (tableNames == null || tableNames.size() == 0) {
                    System.out.println("Cache is not populated.  Please populate cache and restart the filter");
                    System.exit(1);
                }

                System.out.println(tableNames.size() + " events to be loaded from cache");
            }
            else {
                System.out.println("Loading event names from the Hive Metastore");
                HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);
                tableNames = hiveMetaStoreClient.getAllTables(schema);
            }

            ExecutorService executors  = Executors.newFixedThreadPool(10);
            for(final String tableName : tableNames) {

                OrcJSONSchemaDictionaryCache finalCache = cache;
                executors.submit(() -> {
                    System.out.println("Fetching Orc Table: "+tableName);

                    List<FieldSchema> fs = null;
                    try {
                        if (useCache) {
                            fs = finalCache.getCachedSchema(tableName);
                        }
                        else {
                            HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);
                            fs = hiveMetaStoreClient.getSchema(schema, tableName);
                        }

                        if (fs == null || fs.size() == 0) {
                            System.out.println("Table / event schema not found: " + tableName + ". Exiting.");
                            System.exit(1);
                        }
                    } catch (Exception ex) {
                        System.out.println(ex);
                        ex.printStackTrace();
                        System.exit(1);
                    }

                    Set<String> prefixes = new HashSet<>();
                    Map<String, Class> columns = new HashMap<>();

                    for (FieldSchema fieldSchema : fs) {
                        addPrefixes(fieldSchema.getName(), prefixes);
                        columns.put(fieldSchema.getName(), ShokuyokuTypes.getOrcJsonType(fieldSchema.getType()));
                    }

                    if(schemaOverrides!=null && schemaOverrides.containsKey(tableName)){
                        for(String columnName: schemaOverrides.get(tableName).keySet()){
                            addPrefixes(columnName, prefixes);
                            columns.put(columnName, schemaOverrides.get(tableName).get(columnName));
                        }
                    }

                    synchronized (eventTypes) {
                        eventTypes.put(tableName, new EventTypeJSONSchema(prefixes, columns, ignoreNulls, allowInvalidCoercions, columnModifierMap.get(tableName)));
                    }
                    System.out.println("Fetched Orc Table: "+tableName);
                });
            }

            executors.shutdown();

            if (!executors.awaitTermination(10, TimeUnit.MINUTES)) {
                System.out.println("Schema load timed out after 10 minutes");
                System.exit(1);
            }
        } catch (Exception ex) {
            System.err.println("Error fetching Hive Schema Metadata: "+ ex.getMessage());
            ex.printStackTrace();
            System.exit(1);
        }
        finally {
            if (useCache && cache != null)
                cache.closeCache();
        }

        System.out.println("Loaded schemas in " + (System.currentTimeMillis() - startTime) + "ms");
    }

    private static void addPrefixes(String fieldName, Set<String> prefixes) {
        boolean leadingUnderscore = false;
        String base = "";
        for (String part: fieldName.split("_")) {
            if(part.length()==0){
                leadingUnderscore=true;
                continue;
            }
            String nextPart = (leadingUnderscore ? "_" :"") + part;
            base =  base.length() == 0  ? nextPart :  (base+"_"+nextPart);
            prefixes.add(base);

            leadingUnderscore=false;
        }
    }
}
