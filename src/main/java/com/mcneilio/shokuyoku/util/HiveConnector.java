package com.mcneilio.shokuyoku.util;

import com.google.gson.Gson;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.util.*;

public class HiveConnector {
    private static HiveConnector connector = null;

    public static HiveConnector getConnector() {
        if(connector == null) {
            connector = new HiveConnector();
        }
        return connector;
    }

    private HiveConnector() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, System.getenv("HIVE_URL"));
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        try {
            this.client = new HiveMetaStoreClient(hiveConf, null);
        } catch (MetaException e) {
            e.printStackTrace();
        }
        this.gson = new Gson();
    }

    public synchronized  void addPartition(String dbName, String eventType, List<String> values){
        Partition partition = new Partition();
        partition.setDbName(dbName);
        partition.setTableName(eventType);
        partition.setValues(values);
        try {
            client.add_partition(partition);
        } catch (TException e) {
            e.printStackTrace();
        }
    }

    public synchronized String listDatabases() {
        try {
            List<String> tables =    client.getAllDatabases();
            return gson.toJson(tables);
        }catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling
            System.exit(1);
            return null;
        }
    }

    public synchronized String listTables(String db) {
        try {
            List<String> tables = client.getTables(db, "*");
            return gson.toJson(tables);
        }
        catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling
            System.exit(1);
            return null;
        }
    }

    public synchronized String addTable(Table table) {
       // Table table = gson.fromJson(tbl, Table.class);
        try {
            client.createTable(table);
        }
        catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling
            System.exit(1);
        }
        return "";
    }

    public synchronized String addTable(String db, String tbl) {
        Table table = gson.fromJson(tbl, Table.class);
        try {
            client.createTable(table);
        }
        catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling
            System.exit(1);
        }
        return "";
    }

    public synchronized String getTable(String db, String tableName) {
        try {
            Table tbl = client.getTable(db, tableName);
            return gson.toJson(tbl);
        } catch(NoSuchObjectException ex){
            return null;
        }
        catch (Exception e) {
            e.printStackTrace();
            // TODO: error handling
            System.exit(1);
            return null;
        }
    }

    public synchronized void updateTable(String db, String tableName, Table tbl) throws TException {
        client.alter_table(db, tableName, tbl);
    }

    public synchronized String updateTable(String db, String tableName, String tbl) {
        Table table = gson.fromJson(tbl, Table.class);
        try {
            client.alter_table(db, tableName, table);
        }
        catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        return "";
    }

    public synchronized void deleteTable(String db, String tableName) throws TException {
        client.dropTable(db, tableName);
    }

    public synchronized void dropColumns(String db, String tableName, String[] columnNames) throws TException {
        Set<String> columnNameSet = new HashSet<>(Arrays.asList(columnNames));
        Table tbl = client.getTable(db, tableName);

        List<FieldSchema> asd=  tbl.getSd().getCols();
        for(int i=0;i<asd.size();i++){
            if(columnNameSet.contains(asd.get(i).getName())) {
                asd.remove(i--);
            }
        }
        client.alter_table(db, tableName, tbl);
    }

    private HiveMetaStoreClient client;
    private Gson gson;
}
