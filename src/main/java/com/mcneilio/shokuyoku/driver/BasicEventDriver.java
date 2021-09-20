package com.mcneilio.shokuyoku.driver;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.mcneilio.shokuyoku.format.JSONColumnFormat;
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
import java.sql.Timestamp;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.UUID;

public class BasicEventDriver implements EventDriver {

    public BasicEventDriver(String eventName) {
        this.eventName = eventName;
        setTypeDescription();
        this.batch = this.schema.createRowBatch(Integer.parseInt(System.getenv("ORC_BATCH_SIZE")));
        setColumns();
    }

    @Override
    public void addMessage(String m) {
        JSONObject msg = new JSONColumnFormat(new JSONObject(m)).getFlattened();
        int batchPosition = batch.size++;
        msg.keys().forEachRemaining(key -> {
            if(columns.containsKey(key)) {
                if(columns.get(key) instanceof BytesColumnVector && msg.get(key) instanceof java.lang.String) {
                    ((BytesColumnVector) columns.get(key)).setRef(batchPosition,msg.getString(key).getBytes(),
                            0,msg.getString(key).getBytes().length);
                }
                else if(columns.get(key) instanceof LongColumnVector) {
                    LongColumnVector columnVector = (LongColumnVector) columns.get(key);
                    if(msg.get(key) instanceof java.lang.Integer) {
                        columnVector.vector[batchPosition] = msg.getInt(key);
                    }
                    else if(msg.get(key) instanceof java.lang.Boolean) {
                        columnVector.vector[batchPosition] = msg.getBoolean(key) ? 1 : 0;
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
                        ((TimestampColumnVector) columns.get(key)).time[batchPosition] =
                                TimestampColumnVector.getTimestampAsLong(Timestamp.from(Instant.parse(msg.getString(key))));
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
        if (batch.size == batch.getMaxSize()) {
            write();
        }
    }

    @Override
    public void flush() {
        if (batch.size != 0) {
            write();
        }
        if(writer != null) {
            System.out.println("Flushing: " + this.eventName);
            try {
                writer.close();
                writer = null;
                //TODO: S3 info should come from hive location
                s3.putObject(System.getenv("S3_BUCKET"),fileName, new File(fileName));
                this.fileName = null;
            }
            catch (IOException e) {
                System.out.println("Error closing orc file: " + e);
            }
        }
    }

    private void write() {
        try {
            if(this.writer == null) {
                this.fileName = this.eventName + "_" + Instant.now().toEpochMilli() + "_" + UUID.randomUUID() + ".orc";
                this.writer = OrcFile.createWriter(new Path(fileName),
                        OrcFile.writerOptions(conf).setSchema(this.schema));
            }
            this.writer.addRowBatch(batch);
            batch.reset();
            setColumns();
        }
        catch (IOException e) {
            System.out.println("Error writing orc file");
            e.printStackTrace();
        }
        catch (SdkClientException e) {
            System.out.println("Error with AWS SDK");
            e.printStackTrace();
        }
    }

    /*
     * get the schema for this batch
     * TODO: This should pull from hive
     */
    private void setTypeDescription() {
        this.schema = TypeDescription.fromString(System.getenv("ORC_SCHEMA"));
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

    VectorizedRowBatch batch;
    TypeDescription schema;
    HashMap<String, ColumnVector> columns;
    String eventName, fileName;
    Configuration conf = new Configuration();
    Writer writer = null;
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(System.getenv("AWS_DEFAULT_REGION"))).build();
}
