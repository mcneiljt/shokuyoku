package com.mcneilio.shokuyoku.misc;

import com.mcneilio.shokuyoku.driver.BasicEventDriver;
import com.mcneilio.shokuyoku.driver.LocalStorageDriver;
import com.mcneilio.shokuyoku.util.MemoryDescriptionProvider;
import com.mcneilio.shokuyoku.util.TypeDescriptionProvider;
import org.apache.commons.collections.ArrayStack;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


import java.nio.file.Files;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrcTest {

    private void createJSON(String name, String value) {

    }

    @Test
    public void simpleWriteRead() throws Exception {
        String basePath = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();

        Map<String, String> jsonTypes = new HashMap<>();
        jsonTypes.put("zero", "0");
        jsonTypes.put("int", "1234");
        jsonTypes.put("long", "1653185787822");
        jsonTypes.put("float", "1.2343");
        jsonTypes.put("double", "1.23456789123");
        jsonTypes.put("string", "\"cat\"");
        jsonTypes.put("date", "\"2022-03-04\"");
        jsonTypes.put("boolean", "false");



        Map<String, String> orcTypes = new HashMap<>();
        orcTypes.put("boolean", "createBoolean");
        orcTypes.put("byte", "createByte");
        orcTypes.put("short", "createShort");
        orcTypes.put("int", "createInt");
        orcTypes.put("long", "createLong");
        orcTypes.put("float", "createFloat");
        orcTypes.put("double", "createDouble");
        orcTypes.put("decimal", "createDecimal");
        orcTypes.put("string", "createString");
        orcTypes.put("date", "createDate");
        orcTypes.put("timestamp", "createTimestamp");
        orcTypes.put("binary", "createBinary");


        Map<String, Object> expectations = new HashMap<>();
        expectations.put("zero_to_boolean", null);
        expectations.put("zero_to_byte", (byte) 0);
        expectations.put("zero_to_short", (short) 0);
        expectations.put("zero_to_int", (int) 0);
        expectations.put("zero_to_long", (long) 0);
        expectations.put("zero_to_float", (float) 0);
        expectations.put("zero_to_double", (double) 0);
        expectations.put("zero_to_decimal", HiveDecimal.create(0));
        expectations.put("zero_to_string", "0");
        expectations.put("zero_to_date", null);
        expectations.put("zero_to_timestamp", null);
        expectations.put("zero_to_binary", null);

        expectations.put("int_to_boolean", null);
        expectations.put("int_to_byte", (byte) -46);
        expectations.put("int_to_short", (short) 1234);
        expectations.put("int_to_int", (int) 1234);
        expectations.put("int_to_long", (long) 1234);
        expectations.put("int_to_float", (float) 1234);
        expectations.put("int_to_double", (double) 1234);
        expectations.put("int_to_decimal", (double) 1234);
        expectations.put("int_to_string", "1234");
        expectations.put("int_to_date", null);
        expectations.put("int_to_timestamp", null);
        expectations.put("int_to_binary", null);


        expectations.put("long_to_boolean", null);
        expectations.put("long_to_byte", null);
        expectations.put("long_to_short", null);
        expectations.put("long_to_int", null);
        expectations.put("long_to_long", (long) 1653185787822L);
        expectations.put("long_to_float", (float) 1653185787822L);
        expectations.put("long_to_double", (double) 1653185787822L);
        expectations.put("long_to_decimal", (double) 1653185787822L);
        expectations.put("long_to_string", "1653185787822");
        expectations.put("long_to_date", null);
        expectations.put("long_to_timestamp", Instant.ofEpochMilli(1653185787822L));
        expectations.put("long_to_binary", null);


        expectations.put("float_to_boolean", null);
        expectations.put("float_to_byte", (byte) 1);
        expectations.put("float_to_short", (short) 1);
        expectations.put("float_to_int", 1);
        expectations.put("float_to_long", (long) 1L);
        expectations.put("float_to_float", (float) 1.234);
        expectations.put("float_to_double", 1.2343);
        expectations.put("float_to_decimal", 1.2343);
        expectations.put("float_to_string", "1.2343");
        expectations.put("float_to_date", null);
        expectations.put("float_to_timestamp", null);
        expectations.put("float_to_binary", null);


        expectations.put("double_to_boolean", null);
        expectations.put("double_to_byte", (byte) 1);
        expectations.put("double_to_short", (short) 1);
        expectations.put("double_to_int", (int) 1);
        expectations.put("double_to_long", (long) 1L);
        expectations.put("double_to_float", (float) 1.2345678);
        expectations.put("double_to_double", 1.23456789123);
        expectations.put("double_to_decimal", 1.23456789123);
        expectations.put("double_to_string", "1.23456789123");
        expectations.put("double_to_date", null);
        expectations.put("double_to_timestamp", null);
        expectations.put("double_to_binary", null);

        expectations.put("date_to_boolean", null);
        expectations.put("date_to_byte", null);
        expectations.put("date_to_short", null);
        expectations.put("date_to_int", null);
        expectations.put("date_to_long", null);
        expectations.put("date_to_float", null);
        expectations.put("date_to_double", null);
        expectations.put("date_to_decimal", null);
        expectations.put("date_to_string", "2022-03-04");
        expectations.put("date_to_date", LocalDate.parse("2022-03-04"));
        expectations.put("date_to_timestamp", null);
        expectations.put("date_to_binary", null);


        expectations.put("string_to_boolean", null);
        expectations.put("string_to_byte", null);
        expectations.put("string_to_short", null);
        expectations.put("string_to_int", null);
        expectations.put("string_to_long", null);
        expectations.put("string_to_float", null);
        expectations.put("string_to_double", null);
        expectations.put("string_to_decimal", null);
        expectations.put("string_to_string", "cat");
        expectations.put("string_to_date", null);
        expectations.put("string_to_timestamp", null);
        expectations.put("string_to_binary", null);

        expectations.put("boolean_to_boolean", false);
        expectations.put("boolean_to_byte", null);
        expectations.put("boolean_to_short", null);
        expectations.put("boolean_to_int", null);
        expectations.put("boolean_to_long", null);
        expectations.put("boolean_to_float", null);
        expectations.put("boolean_to_double", null);
        expectations.put("boolean_to_decimal", null);
        expectations.put("boolean_to_string", "false");
        expectations.put("boolean_to_date", null);
        expectations.put("boolean_to_timestamp", null);
        expectations.put("boolean_to_binary", null);


        TypeDescription td = TypeDescription.createStruct();
        td = td.addField("id", TypeDescription.createString());
        td = td.addField("date", TypeDescription.createDate());
        TypeDescription adddaasd = (TypeDescription) TypeDescription.class.getMethod("createShort").invoke(TypeDescription.class);


        String jsonObjectStr = "{\"id\":\"my_id\",";
        for (String jsonTypeName : jsonTypes.keySet()) {
            for (String orcTypeName : orcTypes.keySet()) {
                String columnName = jsonTypeName + "_to_" + orcTypeName;
                td = td.addField(columnName, (TypeDescription) TypeDescription.class.getMethod(orcTypes.get(orcTypeName)).invoke(TypeDescription.class));
                jsonObjectStr += "\"" + columnName + "\":" + jsonTypes.get(jsonTypeName) + ",";

                if (!expectations.containsKey(columnName)) {
                    System.out.println("Missing Expectation for: " + columnName);
                    System.exit(1);
                }
            }
        }
        jsonObjectStr += "\"date\":\"2023-01-03\"}";


        MemoryDescriptionProvider memoryDescriptionProvider = new MemoryDescriptionProvider();

        memoryDescriptionProvider.setinstance("test_event", td);

        LocalStorageDriver localStorageDriver = new LocalStorageDriver(basePath);

        TypeDescription typeDescription = memoryDescriptionProvider.getInstance("test_database", "test_event");


        BasicEventDriver basicEventDriver = new BasicEventDriver("test_event", "2022-04-01", typeDescription, localStorageDriver);
        JSONObject jsonObject = new JSONObject(jsonObjectStr);

        basicEventDriver.addMessage(jsonObject);


        String filename = basicEventDriver.flush(false);
        Reader reader = OrcFile.createReader(new Path(filename), OrcFile.readerOptions(new Configuration()));
        RecordReader a = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch(10);
        while (a.nextBatch(batch)) {
            LongColumnVector dateCol = (LongColumnVector) batch.cols[1];
            BytesColumnVector idCol = (BytesColumnVector) batch.cols[0];
            for (int rowNum = 0; rowNum < batch.size; rowNum++) {
                for (int idx = 0; idx < td.getFieldNames().size(); idx++) {
                    String name = td.getFieldNames().get(idx);
                    if (!name.contains("_to_"))
                        continue;
                    String jsonTypeName = name.split("_to_")[0];
                    String orcTypeName = name.split("_to_")[1];
                    System.out.println(name);
                    Object expected = expectations.get(name);

                    if (expected == null) {
                        assertTrue(batch.cols[idx].isNull[rowNum]);
                    } else {

                        if (orcTypeName.equals("date")) {
                            LongColumnVector orcCol = (LongColumnVector) batch.cols[idx];

                            if (expected instanceof LocalDate) {
                                assertTrue(((LocalDate) expected).toEpochDay() == orcCol.vector[rowNum]);
                            } else {
                                assertTrue(false);
                            }
                        } else if (orcTypeName.equals("string")) {
                            BytesColumnVector orcCol = (BytesColumnVector) batch.cols[idx];

                            String str = new String(orcCol.vector[rowNum]);
                            assertTrue(expected.equals(str));

                        } else if (orcTypeName.equals("byte")) {
                            LongColumnVector orcCol = (LongColumnVector) batch.cols[idx];

                            //     String str= new String(orcCol.vector[rowNum]);
                            assertTrue(expected.equals((byte) orcCol.vector[rowNum]));

                        } else if (orcTypeName.equals("short")) {
                            LongColumnVector orcCol = (LongColumnVector) batch.cols[idx];

                            //     String str= new String(orcCol.vector[rowNum]);
                            assertTrue(expected.equals((short) orcCol.vector[rowNum]));

                        } else if (orcTypeName.equals("int")) {
                            LongColumnVector orcCol = (LongColumnVector) batch.cols[idx];

                            //     String str= new String(orcCol.vector[rowNum]);
                            assertTrue(expected.equals((int) orcCol.vector[rowNum]));

                        } else if (orcTypeName.equals("long")) {
                            LongColumnVector orcCol = (LongColumnVector) batch.cols[idx];

                            //     String str= new String(orcCol.vector[rowNum]);
                            assertTrue(expected.equals((long) orcCol.vector[rowNum]));

                        } else if (orcTypeName.equals("double")) {
                            DoubleColumnVector orcCol = (DoubleColumnVector) batch.cols[idx];

                            //     String str= new String(orcCol.vector[rowNum]);
                            assertTrue(expected.equals(orcCol.vector[rowNum]));

                        } else if (orcTypeName.equals("float")) {
                            DoubleColumnVector orcCol = (DoubleColumnVector) batch.cols[idx];

                            if (expected instanceof Float) {
                                assertTrue(expected.toString().substring(0, Math.min(expected.toString().length(), 9)).equals(new Double(orcCol.vector[rowNum]).toString().substring(0, Math.min(expected.toString().length(), 9))));

                            } else if (expected instanceof Integer) {
                                assertTrue(expected.equals(orcCol.vector[rowNum]));

                            } else {
                                assertTrue(false);
                            }


                        } else if (orcTypeName.equals("decimal")) {
                            DecimalColumnVector orcCol = (DecimalColumnVector) batch.cols[idx];

                            if (expected instanceof Double) {
                                assertTrue(expected.equals(orcCol.vector[rowNum].getHiveDecimal().doubleValue()));
                            } else
                                assertTrue(expected.equals(orcCol.vector[rowNum].getHiveDecimal()));

                        } else if (orcTypeName.equals("timestamp")) {
                            TimestampColumnVector orcCol = (TimestampColumnVector) batch.cols[idx];

                            if (expected instanceof Instant) {
                                assertTrue(orcCol.time[rowNum] / 1000 == ((Instant) expected).toEpochMilli() / 1000);
                            } else {
                                assertTrue(false);
                            }

                        } else if (orcTypeName.equals("boolean")) {
                            LongColumnVector orcCol = (LongColumnVector) batch.cols[idx];

                            if (expected instanceof Boolean) {
                                assertTrue((((Boolean)expected) ? 1 : 0) == orcCol.vector[rowNum]);
                            } else {
                                assertTrue(false);
                            }

                        } else {
                            System.out.println("ASD");
                        }
                    }
                }

                System.out.println("Row: " + dateCol.vector[rowNum] + " " + idCol.vector[rowNum].toString());
            }
        }
    }
}
