package com.mcneilio.shokuyoku.driver;


import com.mcneilio.shokuyoku.util.Statsd;
import com.timgroup.statsd.StatsDClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Consumer;

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
        nullColumns();
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

    interface BoolFunction {
        boolean run(Object str);
    }

    private boolean hasCommonType(JSONArray arr, BoolFunction type){
        for(int idx=0;idx<arr.length();idx++){
            if(!type.run(arr.get(idx)))
                return false;
        }
        return true;
    }


    private byte[] bytesForObject(Object obj){
        if(obj instanceof String) {
            return ((String)obj).getBytes();
        }else if(obj instanceof Integer) {
            return ((Integer)obj).toString().getBytes();
        }else if(obj instanceof BigDecimal) {
            return ((BigDecimal)obj).toString().getBytes();
        }else if(obj instanceof Long) {
            return ((Long)obj).toString().getBytes();
        }else if(obj instanceof Boolean) {
            return ((Boolean)((Boolean) obj).booleanValue() ? "true" : "false").toString().getBytes();
        } else if(obj instanceof JSONArray) {
            return ((JSONArray)obj).toString().getBytes();
        }else {
            System.out.println("ASD");
            // TODO: Are there other possible types?
        }
        return null;
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
                        byte[] bytes= bytesForObject(obj);//null;

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
                            longColumnVector.vector[idx] = ((Integer)obj).shortValue();
                            longColumnVector.isNull[idx] = false;
                        }else if(obj instanceof Long) {
                            longColumnVector.vector[idx] = ((Long)obj).shortValue();
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
                        }else if(obj instanceof Long) {
                            longColumnVector.vector[idx] = (Long)obj;
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
                        }else if(obj instanceof Long) {
                            longColumnVector.vector[idx] = (Long)obj;
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
                            doubleColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if(typeDescription.toString().equals("binary")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        BytesColumnVector bytesColumnVector = (BytesColumnVector) columnVector;
                        bytesColumnVector.isNull[idx]=true;
                    }
                };
            }  else if(typeDescription.toString().equals("timestamp")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        TimestampColumnVector timestampColumnVector = (TimestampColumnVector) columnVector;
                        if(obj instanceof Long) {
                            timestampColumnVector.time[idx] = (Long)obj;
                            timestampColumnVector.isNull[idx]=false;
                        }else{
                            timestampColumnVector.isNull[idx]=true;

                        }
                    }
                };
            }else if(typeDescription.toString().equals("array<boolean>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector booleanListColumnVector = (ListColumnVector) columnVector;

                        if(obj instanceof String || obj instanceof BigDecimal) {
                           booleanListColumnVector.isNull[idx]=true;
                        } else if(obj instanceof Boolean) {
                            booleanListColumnVector.isNull[idx]=false;
                            int offset = booleanListColumnVector.childCount;
                            booleanListColumnVector.offsets[idx] = offset;
                            booleanListColumnVector.lengths[idx] = 1;
                            booleanListColumnVector.childCount += 1;
                            booleanListColumnVector.child.ensureSize(booleanListColumnVector.childCount, true);
                            ((LongColumnVector) booleanListColumnVector.child).vector[offset]= (Boolean)obj ? 1 : 0;
                        } else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Boolean)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = booleanListColumnVector.childCount;
                                booleanListColumnVector.offsets[idx] = offset;
                                booleanListColumnVector.lengths[idx] = msgArray.length();
                                booleanListColumnVector.childCount += msgArray.length();
                                booleanListColumnVector.child.ensureSize(booleanListColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((LongColumnVector) booleanListColumnVector.child).vector[offset+i]= msgArray.getBoolean(idx) ? 1 : 0;
                                }
                            } else {
                                booleanListColumnVector.isNull[idx]=true;
                            }
                        }else if(obj instanceof Integer || obj instanceof Long) {
                            booleanListColumnVector.isNull[idx]=true;
                        }else{
                            booleanListColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if(typeDescription.toString().equals("array<float>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector floatColumnVector = (ListColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof String || obj instanceof Boolean) {
                            //timestampColumnVector.time[idx] = (Long)obj;
                            floatColumnVector.isNull[idx]=true;

                        } else if(obj instanceof BigDecimal) {

                            int offset = floatColumnVector.childCount;
                            floatColumnVector.isNull[idx] = false;
                            floatColumnVector.offsets[idx] = offset;
                            floatColumnVector.lengths[idx] = 1;
                            floatColumnVector.childCount += 1;
                            ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal)obj).doubleValue();


                        }else if(obj instanceof Integer) {

                            int offset = floatColumnVector.childCount;
                            floatColumnVector.isNull[idx] = false;
                            floatColumnVector.offsets[idx] = offset;
                            floatColumnVector.lengths[idx] = 1;
                            floatColumnVector.childCount += 1;
                            ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=(Integer)obj;


                        }else if(obj instanceof Long) {

                            int offset = floatColumnVector.childCount;
                            floatColumnVector.isNull[idx] = false;
                            floatColumnVector.offsets[idx] = offset;
                            floatColumnVector.lengths[idx] = 1;
                            floatColumnVector.childCount += 1;
                            ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=(Long)obj;


                        }else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Float || a instanceof Integer  || a instanceof Long|| a instanceof BigDecimal)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = floatColumnVector.childCount;
                                floatColumnVector.offsets[idx] = offset;
                                floatColumnVector.lengths[idx] = msgArray.length();
                                floatColumnVector.childCount += msgArray.length();
                                floatColumnVector.child.ensureSize(floatColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((DoubleColumnVector) floatColumnVector.child).vector[offset+i] = msgArray.getFloat(i);
                                }
                            }else {
                                floatColumnVector.isNull[idx]=true;
                            }
                        }else{
                            floatColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if(typeDescription.toString().equals("array<date>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector dateListColumnVector = (ListColumnVector) columnVector;
                        if(obj instanceof String) {
                            try {
                                int offset = dateListColumnVector.childCount;
                                long tmp = LocalDate.parse((String) obj).toEpochDay();

                                dateListColumnVector.isNull[idx] = false;

                                dateListColumnVector.offsets[idx] = offset;
                                dateListColumnVector.lengths[idx] = 1;
                                dateListColumnVector.childCount += 1;
                                dateListColumnVector.child.ensureSize(dateListColumnVector.childCount, true);

                                ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=tmp;

                            }catch (DateTimeParseException ex) {
                                dateListColumnVector.isNull[idx] = true;
                            }

                        }else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof String)){
                                try {
                                    JSONArray msgArray = (JSONArray) obj;
                                    long[] longs = new long[msgArray.length()];
                                    for(int i=0; i<msgArray.length(); i++) {
                                        longs[i] = LocalDate.parse(msgArray.getString(i)).toEpochDay();
                                    }
                                    int offset = dateListColumnVector.childCount;
                                    dateListColumnVector.offsets[idx] = offset;
                                    dateListColumnVector.lengths[idx] = msgArray.length();
                                    dateListColumnVector.childCount += msgArray.length();
                                    dateListColumnVector.child.ensureSize(dateListColumnVector.childCount, true);
                                    for(int i=0; i<longs.length; i++) {
                                        ((LongColumnVector) dateListColumnVector.child).vector[offset+i] = longs[i];
                                    }
                                    dateListColumnVector.isNull[idx] = false;

                                }catch (DateTimeParseException ex) {
                                    dateListColumnVector.isNull[idx] = true;
                                }
                            }else {
                                dateListColumnVector.isNull[idx]=true;
                            }
                        }else{
                            dateListColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if(typeDescription.toString().equals("array<string>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector timestampColumnVector = (ListColumnVector) columnVector;

                        if (obj instanceof JSONArray){
                            JSONArray msgArray = (JSONArray) obj;
                            byte[][] bytes = new byte[msgArray.length()][];
                            long total=0;
                            for(int i=0; i<msgArray.length(); i++) {
                                bytes[i] = bytesForObject(msgArray.get(i));
                                total+=bytes[i].length;
                            }
                            int offset = timestampColumnVector.childCount;
                            timestampColumnVector.lengths[idx] = msgArray.length();
                            timestampColumnVector.childCount += msgArray.length();
                            timestampColumnVector.child.ensureSize(timestampColumnVector.childCount, true);
                            for(int i=0; i<bytes.length; i++) {
                                ((BytesColumnVector) timestampColumnVector.child).isNull[offset+i]=false;
                                ((BytesColumnVector) timestampColumnVector.child).setRef(offset+i,bytes[i],0,bytes[i].length);
                            }
                            timestampColumnVector.isNull[idx] = false;

                        }else  {
                            byte[] bytes = bytesForObject(obj);
                                int offset = timestampColumnVector.childCount;


                                timestampColumnVector.offsets[idx] = offset;
                                timestampColumnVector.lengths[idx] = 1;
                                timestampColumnVector.childCount += 1;
                                timestampColumnVector.child.ensureSize(timestampColumnVector.childCount, true);
                                    ((BytesColumnVector) timestampColumnVector.child).setRef(offset,bytes,0,bytes.length);
                                timestampColumnVector.isNull[idx] = false;
                        }

                    }
                };
            } else if(typeDescription.toString().equals("array<tinyint>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector timestampColumnVector = (ListColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof String || obj instanceof Boolean) {
                            timestampColumnVector.isNull[idx]=true;
                        } else if(obj instanceof BigDecimal) {
                            int offset = timestampColumnVector.childCount;
                            timestampColumnVector.isNull[idx] = false;
                            timestampColumnVector.offsets[idx] = offset;
                            timestampColumnVector.lengths[idx] = 1;
                            timestampColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal)obj).longValue();
                        } else if(obj instanceof Integer) {
                            int offset = timestampColumnVector.childCount;
                            timestampColumnVector.isNull[idx] = false;
                            timestampColumnVector.offsets[idx] = offset;
                            timestampColumnVector.lengths[idx] = 1;
                            timestampColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer)obj);
                        } else if(obj instanceof Long) {
                            int offset = timestampColumnVector.childCount;
                            timestampColumnVector.isNull[idx] = false;
                            timestampColumnVector.offsets[idx] = offset;
                            timestampColumnVector.lengths[idx] = 1;
                            timestampColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long)obj);
                        } else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long|| a instanceof BigDecimal)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = timestampColumnVector.childCount;
                                timestampColumnVector.offsets[idx] = offset;
                                timestampColumnVector.lengths[idx] = msgArray.length();
                                timestampColumnVector.childCount += msgArray.length();
                                timestampColumnVector.child.ensureSize(timestampColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((LongColumnVector) timestampColumnVector.child).vector[i] = msgArray.getInt(i);
                                }
                            }else {
                                timestampColumnVector.isNull[idx]=true;
                            }
                        }else{
                            timestampColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if(typeDescription.toString().equals("array<double>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector doubleListColumnVector = (ListColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof String || obj instanceof Boolean) {
                            doubleListColumnVector.isNull[idx]=true;
                        } else if(obj instanceof BigDecimal) {
                            int offset = doubleListColumnVector.childCount;
                            doubleListColumnVector.isNull[idx] = false;
                            doubleListColumnVector.offsets[idx] = offset;
                            doubleListColumnVector.lengths[idx] = 1;
                            doubleListColumnVector.childCount += 1;
                            ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal)obj).doubleValue();
                        }  else if(obj instanceof Integer) {
                            int offset = doubleListColumnVector.childCount;
                            doubleListColumnVector.isNull[idx] = false;
                            doubleListColumnVector.offsets[idx] = offset;
                            doubleListColumnVector.lengths[idx] = 1;
                            doubleListColumnVector.childCount += 1;
                            ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer)obj);
                        }  else if(obj instanceof Long) {
                            int offset = doubleListColumnVector.childCount;
                            doubleListColumnVector.isNull[idx] = false;
                            doubleListColumnVector.offsets[idx] = offset;
                            doubleListColumnVector.lengths[idx] = 1;
                            doubleListColumnVector.childCount += 1;
                            ((DoubleColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long)obj);
                        } else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long || a instanceof BigDecimal)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = doubleListColumnVector.childCount;
                                doubleListColumnVector.offsets[idx] = offset;
                                doubleListColumnVector.lengths[idx] = msgArray.length();
                                doubleListColumnVector.childCount += msgArray.length();
                                doubleListColumnVector.child.ensureSize(doubleListColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((DoubleColumnVector) doubleListColumnVector.child).vector[offset+i] = msgArray.getDouble(i);
                                }
                            }else {
                                doubleListColumnVector.isNull[idx]=true;
                            }
                        }else{
                            doubleListColumnVector.isNull[idx]=true;
                        }
                    }
                };
            } else if(typeDescription.toString().startsWith("array<decimal")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector timestampColumnVector = (ListColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof String) {
                            timestampColumnVector.isNull[idx]=true;
                        } else if(obj instanceof BigDecimal) {
                            int offset = timestampColumnVector.childCount;
                            timestampColumnVector.isNull[idx] = false;
                            timestampColumnVector.offsets[idx] = offset;
                            timestampColumnVector.lengths[idx] = 1;
                            timestampColumnVector.childCount += 1;
                            ((DecimalColumnVector)((ListColumnVector) columnVector).child).vector[offset]= new HiveDecimalWritable(HiveDecimal.create((BigDecimal)obj));
                        }else if(obj instanceof Integer) {
                            int offset = timestampColumnVector.childCount;
                            timestampColumnVector.isNull[idx] = false;
                            timestampColumnVector.offsets[idx] = offset;
                            timestampColumnVector.lengths[idx] = 1;
                            timestampColumnVector.childCount += 1;
                            ((DecimalColumnVector)((ListColumnVector) columnVector).child).vector[offset]= new HiveDecimalWritable(HiveDecimal.create((Integer)obj));
                        }else if(obj instanceof Long) {
                            //timestampColumnVector.time[idx] = (Long)obj;
                            //    timestampColumnVector.isNull[idx]=true;
                            int offset = timestampColumnVector.childCount;
                            timestampColumnVector.isNull[idx] = false;
                            timestampColumnVector.offsets[idx] = offset;
                            timestampColumnVector.lengths[idx] = 1;
                            timestampColumnVector.childCount += 1;
                            ((DecimalColumnVector)((ListColumnVector) columnVector).child).vector[offset]= new HiveDecimalWritable(HiveDecimal.create((Long)obj));
                        } else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long || a instanceof BigDecimal)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = timestampColumnVector.childCount;
                                timestampColumnVector.offsets[idx] = offset;
                                timestampColumnVector.lengths[idx] = msgArray.length();
                                timestampColumnVector.childCount += msgArray.length();
                                timestampColumnVector.child.ensureSize(timestampColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((DecimalColumnVector) timestampColumnVector.child).vector[offset+i] = new HiveDecimalWritable(HiveDecimal.create(msgArray.getDouble(i)));
                                }
                            }else {
                                timestampColumnVector.isNull[idx]=true;
                            }
                        }else{
                            timestampColumnVector.isNull[idx]=true;

                        }
                    }
                };
            } else if(typeDescription.toString().equals("array<int>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector intListColumnVector = (ListColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof String || obj instanceof Boolean) {
                         //   timestampColumnVector.time[idx] = (Long)obj;
                            intListColumnVector.isNull[idx]=true;

                        } else if (obj instanceof BigDecimal) {
                            int offset = intListColumnVector.childCount;
                            intListColumnVector.isNull[idx] = false;
                            intListColumnVector.offsets[idx] = offset;
                            intListColumnVector.lengths[idx] = 1;
                            intListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal) obj).longValue();
                        } else if (obj instanceof Integer) {
                            int offset = intListColumnVector.childCount;
                            intListColumnVector.isNull[idx] = false;
                            intListColumnVector.offsets[idx] = offset;
                            intListColumnVector.lengths[idx] = 1;
                            intListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer) obj);
                        }else if (obj instanceof Long) {
                            int offset = intListColumnVector.childCount;
                            intListColumnVector.isNull[idx] = false;
                            intListColumnVector.offsets[idx] = offset;
                            intListColumnVector.lengths[idx] = 1;
                            intListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long) obj).intValue();
                        }else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long|| a instanceof BigDecimal) ){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = intListColumnVector.childCount;
                                intListColumnVector.offsets[idx] = offset;
                                intListColumnVector.lengths[idx] = msgArray.length();
                                intListColumnVector.childCount += msgArray.length();
                                intListColumnVector.child.ensureSize(intListColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((LongColumnVector) intListColumnVector.child).vector[offset+i] = msgArray.getInt(i);
                                }
                            }else {
                                intListColumnVector.isNull[idx]=true;
                            }
                        }else{
                            intListColumnVector.isNull[idx]=true;

                        }
                    }
                };
            }  else if(typeDescription.toString().equals("array<timestamp>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector timestampListColumnVector = (ListColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof String) {
                          //  timestampColumnVector.time[idx] = (Long)obj;
                            timestampListColumnVector.isNull[idx]=true;

                        } else if(obj instanceof BigDecimal){
                            // don't try and make a timestamp out of a bigdecimal
                            timestampListColumnVector.isNull[idx]=true;

                        } else if(obj instanceof Integer){
                            // don't try and make a timestamp out of a Integer
                            timestampListColumnVector.isNull[idx]=true;

                        } else if(obj instanceof Boolean){
                            // don't try and make a timestamp out of a Boolean
                            timestampListColumnVector.isNull[idx]=true;

                        } else if(obj instanceof Long) {

                            int offset = timestampListColumnVector.childCount;
                            timestampListColumnVector.isNull[idx] = false;
                            timestampListColumnVector.offsets[idx] = offset;
                            timestampListColumnVector.lengths[idx] = 1;
                            timestampListColumnVector.childCount += 1;
                            ((TimestampColumnVector)((ListColumnVector) columnVector).child).time[offset]=((Long) obj);

                        }else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Long)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = timestampListColumnVector.childCount;
                                timestampListColumnVector.offsets[idx] = offset;
                                timestampListColumnVector.lengths[idx] = msgArray.length();
                                timestampListColumnVector.childCount += msgArray.length();
                                timestampListColumnVector.child.ensureSize(timestampListColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((TimestampColumnVector) timestampListColumnVector.child).time[offset+i]= msgArray.getLong(i);//, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                                }
                            }else {
                                timestampListColumnVector.isNull[idx]=true;
                            }
                        }else{
                            timestampListColumnVector.isNull[idx]=true;

                        }
                    }
                };
            } else if(typeDescription.toString().equals("array<smallint>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector smallintListColumnVector = (ListColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof String || obj instanceof Boolean) {
                          //  timestampColumnVector.time[idx] = (Long)obj;
                            smallintListColumnVector.isNull[idx]=true;

                        } else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long|| a instanceof BigDecimal)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = smallintListColumnVector.childCount;
                                smallintListColumnVector.offsets[idx] = offset;
                                smallintListColumnVector.lengths[idx] = msgArray.length();
                                smallintListColumnVector.childCount += msgArray.length();
                                smallintListColumnVector.child.ensureSize(smallintListColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((LongColumnVector) smallintListColumnVector.child).vector[offset+i] = (short)msgArray.getInt(i);//, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                                }
                            }else {
                                smallintListColumnVector.isNull[idx]=true;
                            }
                        }else if(obj instanceof BigDecimal) {
                            int offset = smallintListColumnVector.childCount;
                            smallintListColumnVector.isNull[idx] = false;
                            smallintListColumnVector.offsets[idx] = offset;
                            smallintListColumnVector.lengths[idx] = 1;
                            smallintListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal) obj).shortValue();
                        }else if(obj instanceof Integer) {
                            int offset = smallintListColumnVector.childCount;
                            smallintListColumnVector.isNull[idx] = false;
                            smallintListColumnVector.offsets[idx] = offset;
                            smallintListColumnVector.lengths[idx] = 1;
                            smallintListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer) obj).shortValue();
                        }else if(obj instanceof Long) {
                            int offset = smallintListColumnVector.childCount;
                            smallintListColumnVector.isNull[idx] = false;
                            smallintListColumnVector.offsets[idx] = offset;
                            smallintListColumnVector.lengths[idx] = 1;
                            smallintListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long) obj).shortValue();
                        }else{
                            smallintListColumnVector.isNull[idx]=true;
                        }
                    }
                };
            }  else if(typeDescription.toString().equals("array<bigint>")) {
                orcers[i] = new JSONToOrc() {
                    @Override
                    public void addObject(ColumnVector columnVector, int idx, Object obj) {
                        ListColumnVector bigintListColumnVector = (ListColumnVector) columnVector;
                        //byte[] bytes= null;
                        if(obj instanceof String || obj instanceof Boolean) {
             //               timestampColumnVector.time[idx] = (Long)obj;
                            bigintListColumnVector.isNull[idx]=true;

                        } else if(obj instanceof BigDecimal) {
                            int offset = bigintListColumnVector.childCount;
                            bigintListColumnVector.isNull[idx] = false;
                            bigintListColumnVector.offsets[idx] = offset;
                            bigintListColumnVector.lengths[idx] = 1;
                            bigintListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((BigDecimal) obj).longValue();
                        } else if(obj instanceof Integer) {
                            int offset = bigintListColumnVector.childCount;
                            bigintListColumnVector.isNull[idx] = false;
                            bigintListColumnVector.offsets[idx] = offset;
                            bigintListColumnVector.lengths[idx] = 1;
                            bigintListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Integer) obj);
                        } else if(obj instanceof Long) {
                            int offset = bigintListColumnVector.childCount;
                            bigintListColumnVector.isNull[idx] = false;
                            bigintListColumnVector.offsets[idx] = offset;
                            bigintListColumnVector.lengths[idx] = 1;
                            bigintListColumnVector.childCount += 1;
                            ((LongColumnVector)((ListColumnVector) columnVector).child).vector[offset]=((Long) obj);
                        } else if(obj instanceof Integer) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Boolean)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = bigintListColumnVector.childCount;
                                bigintListColumnVector.offsets[idx] = offset;
                                bigintListColumnVector.lengths[idx] = msgArray.length();
                                bigintListColumnVector.childCount += msgArray.length();
                                bigintListColumnVector.child.ensureSize(bigintListColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((LongColumnVector) bigintListColumnVector.child).vector[offset+i] = msgArray.getLong(i);//, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                                }
                            }else {
                                bigintListColumnVector.isNull[idx]=true;
                            }
                        } else if(obj instanceof JSONArray) {
                            if(hasCommonType((JSONArray) obj, (a) -> a instanceof Integer || a instanceof Long|| a instanceof BigDecimal)){
                                JSONArray msgArray = (JSONArray) obj;
                                int offset = bigintListColumnVector.childCount;
                                bigintListColumnVector.offsets[idx] = offset;
                                bigintListColumnVector.lengths[idx] = msgArray.length();
                                bigintListColumnVector.childCount += msgArray.length();
                                bigintListColumnVector.child.ensureSize(bigintListColumnVector.childCount, true);
                                for(int i=0; i<msgArray.length(); i++) {
                                    ((LongColumnVector) bigintListColumnVector.child).vector[offset+i] = msgArray.getLong(i);//, ((String) msgArray.get(i)).getBytes(),0,((String) msgArray.get(i)).getBytes().length);
                                }
                            }else {
                                bigintListColumnVector.isNull[idx]=true;
                            }
                        }else{
                            bigintListColumnVector.isNull[idx]=true;
                        }
                    }
                };
            }   else{
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
