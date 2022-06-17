package com.mcneilio.shokuyoku.driver;


import com.mcneilio.shokuyoku.util.Statsd;
import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;

/**
 * BasicEventDriver takes raw JSONObjects and adds them to an ORC file. On an interval or if a certain size
 * is reached, the ORC file is sent to the storageDriver.
 */
public class BasicEventDriver implements EventDriver {

    public BasicEventDriver(String eventName, String date, TypeDescription typeDescription, StorageDriver storageDriver) {
        this.eventName = eventName;
        this.date = date;
        this.storageDriver = storageDriver;
        this.schema = typeDescription;

        // TODO This env vars should probably be pulled out.
        this.batch = this.schema.createRowBatch(System.getenv("ORC_BATCH_SIZE") != null ? Integer.parseInt(System.getenv("ORC_BATCH_SIZE")) : 1000);
        setColumns();
        nullColumns();
        this.statsd = Statsd.getInstance();
    }

    @Override
    public void addMessage(JSONObject msg) {
        long t = Instant.now().toEpochMilli();
        int batchPosition = batch.size++;

        msg.keys().forEachRemaining(key -> {
            if(msg.isNull(key)){
                return;
            }
            if(columns.containsKey(key)) {
                if(columns.get(key) instanceof BytesColumnVector) {
                    byte[] strBytes = msg.get(key).toString().getBytes();
                    ((BytesColumnVector) columns.get(key)).setRef(batchPosition,strBytes,
                            0,strBytes.length);
                    columns.get(key).isNull[batchPosition] = false;
                }
                else if(columns.get(key) instanceof LongColumnVector) {
                    LongColumnVector columnVector = (LongColumnVector) columns.get(key);
                    if(msg.get(key) instanceof java.lang.Integer) {
                        columnVector.vector[batchPosition] = msg.getInt(key);
                        columns.get(key).isNull[batchPosition] = false;
                    }
                    else if(msg.get(key) instanceof java.lang.Boolean) {
                        columnVector.vector[batchPosition] = msg.getBoolean(key) ? 1 : 0;
                        columns.get(key).isNull[batchPosition] = false;
                    }
                    else {
                        //TODO: unexpected type
                    }
                }
                else if(columns.get(key) instanceof ListColumnVector && msg.get(key) instanceof JSONArray) {
                    ListColumnVector columnVector = (ListColumnVector) columns.get(key);
                    JSONArray msgArray = msg.getJSONArray(key);
                    int offset = columnVector.childCount;
                    columnVector.offsets[batchPosition] = offset;
                    columnVector.lengths[batchPosition] = msgArray.length();
                    columnVector.childCount += msgArray.length();
                    columnVector.child.ensureSize(columnVector.childCount, true);
                    for(int i=0; i<msgArray.length(); i++) {
                        ((BytesColumnVector) columnVector.child).setRef(offset+i, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                    }
                }
                else if(columns.get(key) instanceof TimestampColumnVector) {
                    try {
                        String timeValue = msg.getString(key);
                        long timeInMilliseconds = Instant.parse(timeValue).toEpochMilli();
                        Timestamp timestamp = new Timestamp(timeInMilliseconds);

                        TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columns.get(key);
                        timestampColumnVector.time[batchPosition] = timestamp.getTime();
                    }
                    catch (DateTimeParseException e) {
                        System.out.println("Failed to parse timestamp for: "+key);
                    }
                }
                else {
                    //TODO: complain of type mismatch
                }
            }
            else {
                //TODO: complain about new key
            }
        });
        ((LongColumnVector) columns.get("date")).vector[batchPosition] = LocalDate.parse(date).toEpochDay();
        columns.get("date").isNull[batchPosition] = false;
        statsd.count("message.count", 1, new String[]{"env:"+System.getenv("STATSD_ENV")});
        if (batch.size == batch.getMaxSize()) {
            write();
        }
        statsd.histogram("eventDriver.addMessage.ms", Instant.now().toEpochMilli() - t,
            new String[] {"env:"+System.getenv("STATSD_ENV")});
    }

    @Override
    public String flush(boolean deleteFile) {
        long t = Instant.now().toEpochMilli();
        if (batch.size != 0) {
            write();
        }
        String writtenFileName = this.fileName;

        if(writer != null) {
            System.out.println("Flushing: " + this.eventName);
            try {
                writer.close();
                writer = null;
                if (storageDriver != null){
                    storageDriver.addFile(date,  eventName, fileName, Paths.get(fileName));
                }
                //TODO: create hive partition
                if(deleteFile)
                    new File(fileName).delete();
                this.fileName = null;
            }
            catch (IOException e) {
                System.out.println("Error closing orc file: " + e);
            }
        }
        statsd.histogram("eventDriver.flush.ms", Instant.now().toEpochMilli() - t,
            new String[] {"env:"+System.getenv("STATSD_ENV")});
        return writtenFileName;
    }

    private void write() {
        long t = Instant.now().toEpochMilli();
        try {
            if(this.writer == null) {
                this.fileName = this.eventName + "_" + Instant.now().toEpochMilli() + "_" + UUID.randomUUID() + ".orc";
                this.writer = OrcFile.createWriter(new Path(fileName),
                        OrcFile.writerOptions(conf).setSchema(this.schema));
            }
            this.writer.addRowBatch(batch);
            batch.reset();
            nullColumnsV2();
        }
        catch (IOException e) {
            System.out.println("Error writing orc file");
            e.printStackTrace();
        }

        statsd.histogram("eventDriver.write.ms", Instant.now().toEpochMilli() - t,
            new String[] {"env:"+System.getenv("STATSD_ENV")});
    }

    private void nullColumns() {
        columns.forEach( (key, value) -> {
            value.noNulls = false;

            if(value instanceof LongColumnVector) {
                Arrays.fill(((LongColumnVector) value).vector, LongColumnVector.NULL_VALUE);
                Arrays.fill(value.isNull, true);
            }
            else if(value instanceof BytesColumnVector) {
                Arrays.fill(((BytesColumnVector) value).vector, null);
                Arrays.fill(value.isNull, true);
            }
            //array and timestamp columnVectors don't provide fillWithNulls
            //array and timestamp columnVectors appear to work with null values
        });
    }


    /**
     * The goal here is to tie column vectors to the keys, so they can be easily referenced
     *
     */
    private void setColumns() {
        HashMap<String, ColumnVector> columns = new HashMap<>();
        String[] fields = schema.getFieldNames().toArray(new String[0]);
        ColumnVector[] columnVectors = this.batch.cols;
        for(int i = 0; i < batch.numCols; i++) {
            columns.put(fields[i], columnVectors[i]);
        }
        this.columns = columns;
    }

    public String getFileName(){
        return fileName;
    }


    VectorizedRowBatch batch;
    TypeDescription schema;
    HashMap<String, ColumnVector> columns;
    String eventName, fileName, date;
    Configuration conf = new Configuration();
    Writer writer = null;
    StatsDClient statsd;
    StorageDriver storageDriver;

}
