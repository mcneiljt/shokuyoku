package com.mcneilio.shokuyoku.util;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.json.JSONObject;
import org.json.JSONArray;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import redis.clients.jedis.JedisPooled;

public class OrcJSONSchemaDictionaryCache {

    private JedisPooled jedis = null;

    public OrcJSONSchemaDictionaryCache() {
        connect();
    }

    private void connect() {
        try {
            String host = System.getenv("REDIS_HOST");
            String sslEnabled = System.getenv("REDIS_SSL_ENABLED");
            String port = System.getenv("REDIS_PORT");

            jedis = new JedisPooled(host, Integer.parseInt(port), Boolean.parseBoolean(sslEnabled));
        } catch (Exception ex) {
            System.out.println("Error instantiating and connecting to the REDIS cache: " + ex);
            ex.printStackTrace();
            System.exit(1);
        }
    }

    public void populateCache(){
        String hiveURL = System.getenv("HIVE_URL");
        String databaseName = System.getenv("HIVE_DATABASE");

        System.out.println("HIVE: " + hiveURL + "/" + databaseName);

        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, hiveURL);
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");

        try {
            HiveMetaStoreClient hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);

            List<String> tableNames = hiveMetaStoreClient.getAllTables(databaseName);

            System.out.println("Hive metastore returning " + tableNames.size() + " table names");

            // Set the list of event / tables in the cache

            jedis.set("eventNames", String.join(",", tableNames));

            ExecutorService executors = Executors.newFixedThreadPool(10);
            for(final String tableName : tableNames) {
                executors.submit(() -> {
                    System.out.println("Fetching Orc Table: " + tableName);

                    List<FieldSchema> fs;

                    try {
                        HiveMetaStoreClient hiveMetaStoreClient1 = new HiveMetaStoreClient(hiveConf, null);
                        fs = hiveMetaStoreClient1.getSchema(databaseName, tableName);

                        if (fs == null || fs.size() == 0) {
                            System.out.println("Table / event schema not found: " + tableName + ". Exiting.");
                            System.exit(1);
                        }

                        JSONArray json = new JSONArray(fs);
                        jedis.set(tableName, json.toString());

                        System.out.println("Fetched Orc Table: " + tableName);
                    }
                    catch(TException ex) {
                        System.out.println(ex);
                        ex.printStackTrace();
                        System.exit(1);
                    }

                });
            }

            System.out.println("Wait for executors to complete");

            executors.shutdown();

            if (!executors.awaitTermination(10, TimeUnit.MINUTES)) {
                System.out.println("Schema load timed out after 10 minutes");
                System.exit(1);
            }

            System.out.println("Executors completed");
        }
        catch(Exception ex){
            System.err.println("ex");
            ex.printStackTrace();

            // if an error occurs, exit with a status code of 1
            System.exit(1);
        }
        finally {
            try{
                closeCache();
            }
            catch(Exception ex){
                System.err.println(ex);
                ex.printStackTrace();
            }
        }
    }

    // returns a list<String> of event / table names
    public List<String> getEventNameList() {
        if (jedis == null)
            connect();

        String eventNames = jedis.get("eventNames");
        return eventNames != null ? new ArrayList<>(Arrays.asList(StringUtils.split(eventNames, ','))) : null;
    }

    // returns the List<FieldSchema> for the given event / table name

    public List<FieldSchema> getCachedSchema(String eventName) {
        if (jedis == null) {
            connect();
        }

        String schema = jedis.get(eventName);

        if (schema == null)
            return null;

        JSONArray json = new JSONArray(schema);

        List<FieldSchema> list = new ArrayList<>();

        for (int i = 0; i < json.length(); i++){
            JSONObject obj = (JSONObject) json.get(i);

            FieldSchema fs = new FieldSchema();

            fs.setName(obj.getString("name"));
            fs.setType(obj.getString("type"));

            if (obj.getBoolean("setComment"))
                fs.setComment(obj.getString("comment"));

            list.add(fs);
        }

        return list;
    }

    public void closeCache() {
        if (jedis != null) {
            System.out.println("closing cache connection");
            jedis.close();
        }
    }
}
