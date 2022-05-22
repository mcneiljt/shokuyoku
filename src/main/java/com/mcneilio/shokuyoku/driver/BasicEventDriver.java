package com.mcneilio.shokuyoku.driver;


import com.mcneilio.shokuyoku.util.Statsd;
import com.timgroup.statsd.StatsDClient;
import javolution.io.Struct;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

    private interface JSONToOrc {
        void addObject(ColumnVector columnVector, int idx, Object obj);
    }

    public BasicEventDriver(String eventName, String date, TypeDescription typeDescription, StorageDriver storageDriver) {
        this.eventName = eventName;
        this.date = date;
        this.storageDriver = storageDriver;
        this.schema = typeDescription;

        // TODO This env vars should probably be pulled out.
        this.batch = this.schema.createRowBatch(System.getenv("ORC_BATCH_SIZE") != null ? Integer.parseInt(System.getenv("ORC_BATCH_SIZE")) : 1000);
        setColumns();
        nullColumnsV2();
        this.statsd = Statsd.getInstance();
    }

    private JSONToOrc[] orcers;

    @Override
    public void addMessage(JSONObject msg2) {
        long t = Instant.now().toEpochMilli();
        int batchPosition = batch.size++;

        for(int colId =0;colId<colNames.length;colId++){
            String key=colNames[colId];

            if(!msg2.has(key)) {
                continue;
            }
            Object value = msg2.get(key);

            if(orcers[colId]!=null){
                orcers[colId].addObject(columnVectors[colId], batchPosition, value);
            }
//                if(columns.get(key) instanceof BytesColumnVector && msg.get(key) instanceof java.lang.String) {
//                    ((BytesColumnVector) columns.get(key)).setRef(batchPosition,msg.getString(key).getBytes(),
//                            0,msg.getString(key).getBytes().length);
//                    columns.get(key).isNull[batchPosition] = false;
//                }
//                else if(columns.get(key) instanceof LongColumnVector) {
//                    LongColumnVector columnVector = (LongColumnVector) columns.get(key);
//                    if(msg.get(key) instanceof java.lang.Integer) {
//                        columnVector.vector[batchPosition] = msg.getInt(key);
//                        columns.get(key).isNull[batchPosition] = false;
//                    }
//                    else if(msg.get(key) instanceof java.lang.Boolean) {
//                        columnVector.vector[batchPosition] = msg.getBoolean(key) ? 1 : 0;
//                        columns.get(key).isNull[batchPosition] = false;
//                    }
//                    else {
//                        //TODO: unexpected type
//                        System.out.println("Unexpected Type");
//                    }
//                }
//                else if(columns.get(key) instanceof ListColumnVector && msg.get(key) instanceof JSONArray) {
//                    ListColumnVector columnVector = (ListColumnVector) columns.get(key);
//                    JSONArray msgArray = msg.getJSONArray(key);
//                    int offset = columnVector.childCount;
//                    columnVector.offsets[batchPosition] = offset;
//                    columnVector.lengths[batchPosition] = msgArray.length();
//                    columnVector.childCount += msgArray.length();
//                    columnVector.child.ensureSize(columnVector.childCount, true);
//                    for(int i=0; i<msgArray.length(); i++) {
//                        ((BytesColumnVector) columnVector.child).setRef(offset+i, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
//                    }
//                }
//                else if(columns.get(key) instanceof TimestampColumnVector) {
//                    try {
//                        String timeValue = msg.getString(key);
//                        long timeInMilliseconds = Instant.parse(timeValue).toEpochMilli();
//                        Timestamp timestamp = new Timestamp(timeInMilliseconds);
//
//                        TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columns.get(key);
//                        timestampColumnVector.time[batchPosition] = timestamp.getTime();
//                    }
//                    catch (DateTimeParseException e) {
//                        System.out.println("Failed to parse timestamp for: "+key);
//                    } catch (JSONException e) {
//                        System.out.println("Failed to fetch JSONObject for timestamp for: "+key+" "+msg.get(key).getClass()+" "+msg.get(key).toString());
//                    }
//                }
//                else {
//                    //TODO: complain of type mismatch
//                    System.out.println("Unexpected Type");
//
//                }
//            }
//            else {
//                //TODO: complain about new key
//                System.out.println("Unexpected Type");
//
//            }
        }
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
            nullColumns();
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
            if(value instanceof LongColumnVector)
                ((LongColumnVector) value).fillWithNulls();
            else if(value instanceof BytesColumnVector)
                ((BytesColumnVector) value).fillWithNulls();
            //array and timestamp columnVectors don't provide fillWithNulls
            //array and timestamp columnVectors appear to work with null values
        });
    }

    private void nullColumnsV2() {
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
       // String[] fields = schema.getFieldNames().toArray(new String[0]);
        colNames = schema.getFieldNames().toArray(new String[0]);
        columnVectors = this.batch.cols;
        orcers = new JSONToOrc[batch.numCols];

        for(int i = 0; i < colNames.length; i++) {
            columns.put(colNames[i], columnVectors[i]);

            TypeDescription typeDescription =schema.getChildren().get(i);
            if(typeDescription.toString().equals("date")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        LongColumnVector longColumnVector = (LongColumnVector) columnVector;

                        if (obj instanceof String) {
                            try {
                                longColumnVector.vector[idx] = LocalDate.parse((String) obj).toEpochDay();
                                longColumnVector.isNull[idx] = false;
                            }catch (DateTimeParseException ex) {
                                longColumnVector.isNull[idx] = true;
                            }
                        } else {
                            longColumnVector.isNull[idx] = true;
                        }
                    }
                };
            } else if(typeDescription.toString().equals("string")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
                        byte[] bytes= null;
                        if(obj instanceof String) {
                            bytes =((String)obj).getBytes();
                        }else if(obj instanceof Integer) {
                            bytes =((Integer)obj).toString().getBytes();
                        }else if(obj instanceof BigDecimal) {
                            bytes =((BigDecimal)obj).toString().getBytes();
                        }else if(obj instanceof Long) {
                            bytes =((Long)obj).toString().getBytes();
                        }else if(obj instanceof Boolean) {
                            bytes =((Boolean)((Boolean) obj).booleanValue() ? "true" : "false").toString().getBytes();
                        } else {
                              // TODO: Are there other possible types?
                        }
                        bytesColumnVector.setRef(idx, bytes, 0,bytes.length);
                        bytesColumnVector.isNull[idx]=false;
                    }
                };
            }else if(typeDescription.toString().equals("boolean")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        LongColumnVector longColumnVector = (LongColumnVector) columnVector;
                        byte[] bytes= null;
                        if(obj instanceof Boolean) {
                            if(((Boolean) obj).booleanValue()){
                                longColumnVector.vector[idx] = 1;
                                longColumnVector.isNull[idx] = false;
                            }else {
                                longColumnVector.vector[idx] = 0;
                                longColumnVector.isNull[idx] = false;
                            }
                        }else{
                            longColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if( typeDescription.toString().equals("smallint")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        LongColumnVector longColumnVector = (LongColumnVector) columnVector;
                        byte[] bytes= null;
                        if(obj instanceof Integer) {
                            longColumnVector.vector[idx] = (Integer)obj;
                            longColumnVector.isNull[idx] = false;
                        }else{
                            longColumnVector.isNull[idx]=true;
                        }
                    }
                };
            }
            else if(typeDescription.toString().equals("tinyint") ) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        LongColumnVector longColumnVector = (LongColumnVector) columnVector;
                        byte[] bytes= null;
                        if(obj instanceof Integer) {
                            longColumnVector.vector[idx] = (Integer)obj;
                            longColumnVector.isNull[idx] = false;
                        } else if(obj instanceof BigDecimal) {
                            longColumnVector.vector[idx] = ((BigDecimal)obj).longValue();
                            longColumnVector.isNull[idx] = false;
                        }else{
                            longColumnVector.isNull[idx]=true;
                        }
                    }
                };
            }
            else if(typeDescription.toString().equals("int")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        LongColumnVector longColumnVector = (LongColumnVector) columnVector;
                        byte[] bytes= null;
                        if(obj instanceof Integer) {
                            longColumnVector.vector[idx] = (Integer)obj;
                            longColumnVector.isNull[idx] = false;
                        } else if(obj instanceof BigDecimal) {
                            longColumnVector.vector[idx] = ((BigDecimal)obj).longValue();
                            longColumnVector.isNull[idx] = false;
                        }else{
                            longColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if( typeDescription.toString().equals("bigint")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        LongColumnVector longColumnVector = (LongColumnVector) columnVector;
                        byte[] bytes= null;
                        if(obj instanceof Integer) {
                            longColumnVector.vector[idx] = (Integer)obj;
                            longColumnVector.isNull[idx] = false;
                        } else if(obj instanceof BigDecimal) {
                            longColumnVector.vector[idx] = ((BigDecimal)obj).longValue();
                            longColumnVector.isNull[idx] = false;
                        } else if(obj instanceof Long) {
                            longColumnVector.vector[idx] = ((Long)obj).longValue();
                            longColumnVector.isNull[idx] = false;
                        }else{
                            longColumnVector.isNull[idx]=true;
                        }
                    }
                };
            }   else if(typeDescription.toString().startsWith("decimal(")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        DecimalColumnVector decimalColumnVector = (DecimalColumnVector) columnVector;


//                        byte[] bytes= null;
                        if(obj instanceof Integer) {
                            decimalColumnVector.vector[idx]= new HiveDecimalWritable(HiveDecimal.create((Integer)obj));
                            decimalColumnVector.isNull[idx]=false;

                        } else if(obj instanceof BigDecimal) {
                            decimalColumnVector.vector[idx]= new HiveDecimalWritable(HiveDecimal.create((BigDecimal)obj));
                            decimalColumnVector.isNull[idx]=false;

                        } else if(obj instanceof Long) {
                            decimalColumnVector.vector[idx]= new HiveDecimalWritable(HiveDecimal.create((Long)obj));
                            decimalColumnVector.isNull[idx]=false;

                        } else{
                            decimalColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if(typeDescription.toString().equals("double") || typeDescription.toString().equals("float")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        DoubleColumnVector doubleColumnVector = (DoubleColumnVector) columnVector;
                        byte[] bytes= null;
                        if(obj instanceof Integer) {
                            doubleColumnVector.vector[idx]=(Integer)obj;
                            doubleColumnVector.isNull[idx]=false;

                        } else if(obj instanceof Long) {
                            doubleColumnVector.vector[idx]=(Long)obj;
                            doubleColumnVector.isNull[idx]=false;

                        } else if(obj instanceof BigDecimal) {
                            doubleColumnVector.vector[idx]=((BigDecimal)obj).doubleValue();
                            doubleColumnVector.isNull[idx]=false;

                        } else{
                            System.out.println("asd");
                            doubleColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if(typeDescription.toString().equals("binary")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
//                        byte[] bytes= null;
//                        if(obj instanceof Boolean) {
//                            bytes =((String)obj).getBytes();
//                        }else{
//                            System.out.println("asd");
//                        }
                        bytesColumnVector.isNull[idx]=true;
                    }
                };
            }  else if(typeDescription.toString().equals("timestamp")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof Long) {
                            timestampColumnVector.time[idx] = (Long)obj;
                            timestampColumnVector.isNull[idx]=false;

                        }else{
                            System.out.println("asd");
                            timestampColumnVector.isNull[idx]=true;

                        }
                    }
                };
            } else{
                System.out.println("asd");

            }
        }
        this.columns = columns;
    }

    public String getFileName(){
        return fileName;
    }

String[] colNames;
    VectorizedRowBatch batch;
    TypeDescription schema;
    HashMap<String, ColumnVector> columns;
    String eventName, fileName, date;
    Configuration conf = new Configuration();
    Writer writer = null;
    StatsDClient statsd;
    StorageDriver storageDriver;
    ColumnVector[] columnVectors;
}
