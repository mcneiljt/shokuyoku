package com.mcneilio.shokuyoku.driver;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.thrift.TException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public class BasicEventDriver implements EventDriver {

    public BasicEventDriver(String eventName, String date) {
        this.eventName = eventName;
        this.date = date;
        setTypeDescription();
        this.batch = this.schema.createRowBatch(Integer.parseInt(System.getenv("ORC_BATCH_SIZE")));
        setColumns();
        nullColumns();
    }

    @Override
    public void addMessage(JSONObject msg) {
        int batchPosition = batch.size++;
        msg.keys().forEachRemaining(key -> {
            if(columns.containsKey(key)) {
                if(columns.get(key) instanceof BytesColumnVector && msg.get(key) instanceof java.lang.String) {
                    ((BytesColumnVector) columns.get(key)).setRef(batchPosition,msg.getString(key).getBytes(),
                            0,msg.getString(key).getBytes().length);
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
        ((LongColumnVector) columns.get("date")).vector[batchPosition] = LocalDate.parse(date).toEpochDay();
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
                s3.putObject(System.getenv("S3_BUCKET"),
                        System.getenv("S3_PREFIX") + "/" + System.getenv("HIVE_DATABASE") + "/"
                                + eventName + "/" + date + "/" + fileName,
                        new File(fileName));
                //TODO: create hive partition
                new File(fileName).delete();
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
            nullColumns();
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

    private void nullColumns() {
        columns.forEach( (key, value) -> {
            value.noNulls = false;
            if(value instanceof LongColumnVector)
                ((LongColumnVector) value).fillWithNulls();
            else if(value instanceof BytesColumnVector)
                ((BytesColumnVector) value).fillWithNulls();
            //array and timestamp columnVectors don't provide fillWithNulls
            //array and timestamp columnVectors appear to work with null values
        });
    }

    /*
     * get the schema for this batch
     * TODO: This should pull from hive
     */
    private void setTypeDescription() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.local", "false");

        hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, System.getenv("HIVE_URL"));
        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        try {
            Warehouse warehouse = new Warehouse(hiveConf);
        } catch (MetaException e) {
            e.printStackTrace();
        }
        HiveMetaStoreClient hiveMetaStoreClient = null;
        try {
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf, null);
        } catch (MetaException e) {
            e.printStackTrace();
        }
        try {
            String tableName = this.eventName.substring(this.eventName.lastIndexOf(".")+1);
            List<FieldSchema> a = hiveMetaStoreClient.getSchema("events", tableName);
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
                    System.out.println("ASD123");
                }
            }
            this.schema = td;
            System.out.println("ASD");
            return;
        } catch (TException e) {
            e.printStackTrace();
        }


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
    String eventName, fileName, date;
    Configuration conf = new Configuration();
    Writer writer = null;
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(System.getenv("AWS_DEFAULT_REGION"))).build();
}
