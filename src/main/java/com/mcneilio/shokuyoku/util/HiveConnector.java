package com.mcneilio.shokuyoku.util;

import com.google.gson.Gson;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

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

    public String listDatabases() {
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

    public String listTables(String db) {
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

    public String addTable(Table table) {
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

    public String addTable(String db, String tbl) {
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

    public String getTable(String db, String tableName) {
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

    public void updateTable(String db, String tableName, Table tbl) throws TException {
        client.alter_table(db, tableName, tbl);
    }

    public String updateTable(String db, String tableName, String tbl) {
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

    private HiveMetaStoreClient client;
    private Gson gson;
}
