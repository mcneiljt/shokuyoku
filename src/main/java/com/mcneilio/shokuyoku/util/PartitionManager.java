package com.mcneilio.shokuyoku.util;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

public class PartitionManager {
    private static final Map<String, java.util.Set<String>> createdPartitions = new HashMap<>();

    public static synchronized void createPartitionIfNeeded (String tablePath, String eventName, String eventDate) {
        HiveConnector.getConnector();
        HiveMetaStoreClient hsmc = HiveConnector.getHsmc();

        List<String> values = Collections.singletonList(eventDate);
        String dbName = System.getenv("HIVE_DATABASE");
        String partitionLocation = tablePath + "/" + "date=" + eventDate;

        createdPartitions.putIfAbsent(eventName, new java.util.HashSet<>());
        String partitionIdentifier = eventName + "_" + eventDate;
        Set<String> partitionsForTable = createdPartitions.get(eventName);

        if (!partitionsForTable.contains(partitionIdentifier)) {
            System.out.println("Creating partition for " + eventName + " with date " + eventDate + " at " + partitionLocation);

            try {
                Table table = hsmc.getTable(dbName, eventName);
                StorageDescriptor sd = new StorageDescriptor(table.getSd());
                sd.setLocation(partitionLocation);

                Partition partition = new Partition(values, dbName, eventName, 0, 0, sd, null);
                hsmc.add_partition(partition);

                partitionsForTable.add(partitionIdentifier);
                System.out.println("Partition created for " + eventName + " with date " + eventDate + " at " + partitionLocation);
            } catch (NoSuchObjectException e) {
                System.err.println("Table not found in hive: " + dbName + "." + eventName);
            } catch (TException e) {
                e.printStackTrace();
            }
        }
    }
}
