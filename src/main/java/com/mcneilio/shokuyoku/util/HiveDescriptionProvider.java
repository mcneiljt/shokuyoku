package com.mcneilio.shokuyoku.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.orc.TypeDescription;
import org.apache.thrift.TException;

import java.util.List;

public class HiveDescriptionProvider extends MemoryDescriptionProvider {

    public HiveDescriptionProvider() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, System.getenv("HIVE_URL"));
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        try {
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);
        } catch (MetaException e) {
            e.printStackTrace();
        }
    }

    public  TypeDescription getInstance(String databaseName, String eventName) {
        TypeDescription typeDescription = super.getInstance(databaseName, eventName);
        if (typeDescription!=null){
            return typeDescription;
        }

        try {
            // TODO maybe this should be in the caller
            String tableName = eventName.substring(eventName.lastIndexOf(".")+1);
            List<FieldSchema> a = hiveMetaStoreClient.getSchema(databaseName, tableName);
            TypeDescription td=  TypeDescription.createStruct();
            for(FieldSchema fieldSchma : a){
                TypeDescription fieldTypeDescription = TypeDescription.fromString(fieldSchma.getType());
                if(fieldTypeDescription!=null) {
                    td.addField(fieldSchma.getName(), fieldTypeDescription);
                }
            }
            return td;
        } catch (TException e) {
            e.printStackTrace();
        }
        // TODO probably should escalate the exception
        return null;
    }

    private HiveMetaStoreClient hiveMetaStoreClient = null;
}
