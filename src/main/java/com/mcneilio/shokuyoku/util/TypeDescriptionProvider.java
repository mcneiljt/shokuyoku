package com.mcneilio.shokuyoku.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.orc.TypeDescription;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeDescriptionProvider {

    private static Map<String,TypeDescription> typeDescriptions = null;

    public static TypeDescription getInstance(String databaseName, String eventName) {
        if (typeDescriptions!=null ) {
            if(typeDescriptions.containsKey(eventName)) {
                return typeDescriptions.get(eventName);
            }
        } else {
                typeDescriptions=new HashMap<>();

        }
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
            // TODO maybe this should be in the caller
            String tableName = eventName.substring(eventName.lastIndexOf(".")+1);
            List<FieldSchema> a = hiveMetaStoreClient.getSchema(databaseName, tableName);
            TypeDescription td=  TypeDescription.createStruct();
            for(FieldSchema fieldSchma : a){
                if(fieldSchma.getType().equals("string")) {
                    td = td.addField(fieldSchma.getName(), TypeDescription.createString());
                } else if(fieldSchma.getType().equals("boolean")) {
                    td = td.addField(fieldSchma.getName(), TypeDescription.createBoolean());
                }  else if(fieldSchma.getType().equals("timestamp")) {
                    td = td.addField(fieldSchma.getName(), TypeDescription.createTimestamp());
                } else if(fieldSchma.getType().equals("bigint")) {
                    td = td.addField(fieldSchma.getName(), TypeDescription.createLong());
                } else if(fieldSchma.getType().equals("date")) {
                    td = td.addField(fieldSchma.getName(), TypeDescription.createDate());
                } else if(fieldSchma.getType().equals("array<string>")) {
                    // TODO figure out this mapping
                } else {
                    System.out.println("Failed to resolve schema field type.");
                }
            }
            return td;
        } catch (TException e) {
            e.printStackTrace();
        }
        // TODO probably should escalate the exception
        return null;
    }

    public static void setinstance(String eventName, TypeDescription typeDescription){
        if(typeDescriptions==null){
            typeDescriptions=new HashMap<>();
        }
        typeDescriptions.put(eventName, typeDescription);
    }
}
