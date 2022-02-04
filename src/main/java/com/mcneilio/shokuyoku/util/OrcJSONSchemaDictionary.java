package com.mcneilio.shokuyoku.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.lang.reflect.Array;
import java.util.*;

public class OrcJSONSchemaDictionary {

   public  OrcJSONSchemaDictionary(){

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, System.getenv("HIVE_URL"));
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        HiveMetaStoreClient hiveMetaStoreClient = null;
        try {
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);
        } catch (MetaException e) {
            e.printStackTrace();
        }
        try {
            List<String> tableNames = hiveMetaStoreClient.getAllTables("default");
            for(String tableName : tableNames) {
                List<FieldSchema> a = hiveMetaStoreClient.getSchema("default", "test_table_two");

                Set<String> prefixes = new HashSet<>();
                Map<String, Class> columns = new HashMap<>();
                for (FieldSchema fieldSchema : a) {
                    String[] parts = fieldSchema.getName().split("_");

                    String base = "";
                    for (String pieceOne : parts) {
                        if (base.length() == 0) {
                            base = pieceOne;
                        } else {
                            base += "_" + pieceOne;
                        }
                        prefixes.add(base);
                    }

                    // int types
                    columns.put(fieldSchema.getName(), getOrcJsonType(fieldSchema.getType()));
                }
                System.out.println("AS");
            }
        } catch (Exception e) {
            System.out.println("AS");
        }
    }

    private static Class getOrcJsonType(String orcType) {
        if (orcType.startsWith("array<")) {
            String tmp = orcType.substring(6, orcType.length() - 1);
            return Array.newInstance(getOrcJsonType(tmp), 0).getClass();
        } else if (orcType.equals("tinyint")) {
            return Integer.class;
        } else if (orcType.equals("smallint")) {
            return Integer.class;
        } else if (orcType.equals("int")) {
            return Integer.class;
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
        else if (orcType.equals("string") || orcType.equals("timestamp")) {
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