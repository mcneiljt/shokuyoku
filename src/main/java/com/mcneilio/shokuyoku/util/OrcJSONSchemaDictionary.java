package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.thrift.TException;

import java.lang.reflect.Array;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class OrcJSONSchemaDictionary extends JSONSchemaDictionary {

   public  OrcJSONSchemaDictionary(String hiveURL, String databaseName){

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveURL);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        try {
            HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);

            String schema = "events";
            List<String> tableNames = hiveMetaStoreClient.getAllTables(schema);

            ExecutorService executors  = Executors.newFixedThreadPool(10);
            for(final String tableName : tableNames) {

                executors.submit(new Runnable() {
                    @Override
                    public void run() {
                        System.out.println("Fetching Orc Table: "+tableName);

                        List<FieldSchema> a = null;
                        try {
                            HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);
                            a = hiveMetaStoreClient.getSchema(schema, tableName);
                        } catch (TException e) {
                            e.printStackTrace();
                        }

                        Set<String> prefixes = new HashSet<>();
                        Map<String, Class> columns = new HashMap<>();

                        for (FieldSchema fieldSchema : a) {
                            boolean leadingUnderscore = false;
                            String base = "";
                            for(String part: fieldSchema.getName().split("_")){
                                if(part.length()==0){
                                    leadingUnderscore=true;
                                    continue;
                                }
                                String nextPart = (leadingUnderscore ? "_" :"") + part;
                                base =  base.length() == 0  ? nextPart :  (base+"_"+nextPart);
                                prefixes.add(base);

                                leadingUnderscore=false;
                            }
//                            String[] parts = fieldSchema.getName().split("_");

//                            String base = "";
//                            for (String pieceOne : parts) {
//                                if (base.length() == 0) {
//                                    base = pieceOne;
//                                } else {
//                                    base += "_" + pieceOne;
//                                }
//                            }

                            // int types
                            columns.put(fieldSchema.getName(), getOrcJsonType(fieldSchema.getType()));
                        }
                        synchronized (eventTypes) {
                            eventTypes.put(tableName, new EventTypeJSONSchema(prefixes, columns));
                        }
                        System.out.println("Fetched Orc Table: "+tableName);
                    }
                });
            }
            executors.shutdown();
            executors.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            System.err.println("Error fetching types: "+ e.getMessage());
        }
    }

    private static Class getOrcJsonType(String orcType) {
        if (orcType.startsWith("array<")) {
            String tmp = orcType.substring(6, orcType.length() - 1);
            return Array.newInstance(getOrcJsonType(tmp), 0).getClass();
        } else if (orcType.equals("tinyint")) {
            return Double.class;
        } else if (orcType.equals("smallint")) {
            return Double.class;
        } else if (orcType.equals("int")) {
            return Double.class;
        } else if (orcType.equals("bigint")) {
            return Double.class;
        }

        // decimal types
        else if (orcType.equals("float")) {
            return Double.class;
        } else if (orcType.equals("double")) {
            return Double.class;
        } else if (orcType.startsWith("decimal(")) {
            return Double.class;
        }

        // string types
        else if (orcType.equals("string") || orcType.equals("timestamp") || orcType.equals("date")) {
            return String.class;
        } else if (orcType.startsWith("varchar(")) {
            return String.class;
        } else if (orcType.equals("boolean")) {
            return Boolean.class;
        } else {
            System.out.println("Unsupported Column Type: " + orcType);
            return null;
        }
    }

}
