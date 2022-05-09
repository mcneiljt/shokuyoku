package com.mcneilio.shokuyoku.model;

import java.util.List;

public class CreateTableRequest {

    public static class PartitionKey {
        private String columnName;
        private String type;

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    private String name;
    private String location;

    private List<PartitionKey> partitionedBy;


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

    public List<PartitionKey> getPartitionedBy() {
        return partitionedBy;
    }

    public void setPartitionedBy(List<PartitionKey> partitionedBy) {
        this.partitionedBy = partitionedBy;
    }
}
