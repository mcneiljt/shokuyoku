package com.mcneilio.shokuyoku.model;

import org.apache.hadoop.hive.metastore.api.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTableRequest {

    public Table toHiveTable(String database) {
        Table tbl = new Table();
        tbl.setDbName(database);
        tbl.setTableName(this.getName());
        tbl.setTableType("EXTERNAL_TABLE");

        List<FieldSchema> partitionFieldSchemas = new ArrayList<>();
        for (CreateTableRequest.TableColumn partitionKey : this.getPartitionedBy()) {
            FieldSchema fieldSchema = new FieldSchema();
            fieldSchema.setName(partitionKey.getName());
            fieldSchema.setType(partitionKey.getType());
            partitionFieldSchemas.add(fieldSchema);
        }

        tbl.setPartitionKeys(partitionFieldSchemas);

        StorageDescriptor sd = new StorageDescriptor();
        sd.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        sd.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");

        SerDeInfo serDeInfo = new SerDeInfo();
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        Map<String, String> serParams = new HashMap<>();
        serParams.put("serialization.format","1");
        serDeInfo.setParameters(serParams);
        sd.setSerdeInfo(serDeInfo);
        sd.setLocation(this.getLocation());
        sd.setSkewedInfo(new SkewedInfo());


        List<FieldSchema> columns = new ArrayList<>();
        for (CreateTableRequest.TableColumn column: this.getColumns()){
            columns.add(new FieldSchema(column.getName(), column.getType(), ""));
        }
        sd.setCols(columns);
        tbl.setSd(sd);

        Map<String, String> tableParams = new HashMap<>();
        tableParams.put("EXTERNAL", "TRUE");
        tbl.setParameters(tableParams);
        return tbl;
    }

    public static class TableColumn {
        private String name;
        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    public static class TableRequestColumn {

    }

    private String name;
    private String location;

    private List<TableColumn> partitionedBy;

    private List<TableColumn> columns;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public List<TableColumn> getPartitionedBy() {
        return partitionedBy;
    }

    public void setPartitionedBy(List<TableColumn> partitionedBy) {
        this.partitionedBy = partitionedBy;
    }

    public List<TableColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<TableColumn> columns) {
        this.columns = columns;
    }
}
