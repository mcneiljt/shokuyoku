package com.mcneilio.shokuyoku.misc;

import com.mcneilio.shokuyoku.driver.BasicEventDriver;
import com.mcneilio.shokuyoku.driver.LocalStorageDriver;
import com.mcneilio.shokuyoku.util.MemoryDescriptionProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.kafka.common.protocol.types.Field;
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
import java.util.*;

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
        jsonTypes.put("arr_string", "[\"a\",\"b\"]");
        jsonTypes.put("arr_int", "[1234,4321]");
        jsonTypes.put("arr_long", "[1653185787822,1653185787824]");
        jsonTypes.put("arr_float", "[1.2343,1.3343]");
        jsonTypes.put("arr_double", "[1.23456789123,1.33456789123]");
        jsonTypes.put("arr_zero", "[0,0]");
        jsonTypes.put("arr_zero", "[0,0]");
        jsonTypes.put("arr_date", "[\"2022-03-04\",\"2022-03-05\"]");



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

        orcTypes.put("array<boolean>", "createBoolean");
        orcTypes.put("array<byte>", "createByte");
        orcTypes.put("array<short>", "createShort");
        orcTypes.put("array<int>", "createInt");
        orcTypes.put("array<long>", "createLong");
        orcTypes.put("array<float>", "createFloat");
        orcTypes.put("array<double>", "createDouble");
        orcTypes.put("array<decimal>", "createDecimal");
        orcTypes.put("array<string>", "createString");
        orcTypes.put("array<date>", "createDate");
        orcTypes.put("array<timestamp>", "createTimestamp");

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
        expectations.put("zero_to_array<boolean>", null);
        expectations.put("zero_to_array<float>", Collections.singletonList((float) 0));
        expectations.put("zero_to_array<date>", null);
        expectations.put("zero_to_array<string>", Collections.singletonList("0"));
        expectations.put("zero_to_array<byte>", Collections.singletonList((byte) 0));
        expectations.put("zero_to_array<double>", Collections.singletonList((double) 0));
        expectations.put("zero_to_array<decimal>", Collections.singletonList((double) 0));
        expectations.put("zero_to_array<int>", Collections.singletonList((int) 0));
        expectations.put("zero_to_array<timestamp>", null);
        expectations.put("zero_to_array<short>", Collections.singletonList((short) 0));
        expectations.put("zero_to_array<long>", Collections.singletonList((long) 0));

        expectations.put("arr_zero_to_boolean", null);
        expectations.put("arr_zero_to_byte", null);
        expectations.put("arr_zero_to_short", null);
        expectations.put("arr_zero_to_int", null);
        expectations.put("arr_zero_to_long", null);
        expectations.put("arr_zero_to_float", null);
        expectations.put("arr_zero_to_double", null);
        expectations.put("arr_zero_to_decimal", null);
        expectations.put("arr_zero_to_string", "[0,0]");
        expectations.put("arr_zero_to_date", null);
        expectations.put("arr_zero_to_timestamp", null);
        expectations.put("arr_zero_to_binary", null);
        expectations.put("arr_zero_to_array<boolean>", null);
        expectations.put("arr_zero_to_array<float>", Arrays.asList(new Float[]{(float)0,(float)0}));
        expectations.put("arr_zero_to_array<date>", null);
        expectations.put("arr_zero_to_array<string>", Arrays.asList(new String[]{"0","0"}));
        expectations.put("arr_zero_to_array<byte>", Arrays.asList(new Byte[]{(byte)0,(byte)0}));
        expectations.put("arr_zero_to_array<double>", Arrays.asList(new Double[]{(double)0,(double)0}));
        expectations.put("arr_zero_to_array<decimal>", Arrays.asList(new Double[]{(double)0,(double)0}));
        expectations.put("arr_zero_to_array<int>", Arrays.asList(new Integer[]{(int)0,(int)0}));
        expectations.put("arr_zero_to_array<timestamp>", null);
        expectations.put("arr_zero_to_array<short>", Arrays.asList(new Short[]{(short)0,(short)0}));
        expectations.put("arr_zero_to_array<long>", Arrays.asList(new Long[]{(long)0,(long)0}));

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
        expectations.put("int_to_array<boolean>", null);
        expectations.put("int_to_array<float>", Arrays.asList(new Float[]{(float)1234} ));
        expectations.put("int_to_array<date>", null);
        expectations.put("int_to_array<string>", Arrays.asList(new String[]{"1234"} ));
        expectations.put("int_to_array<byte>",Collections.singletonList((byte) -46));
        expectations.put("int_to_array<double>", Arrays.asList(new Double[]{(double)1234}));
        expectations.put("int_to_array<decimal>",  Arrays.asList(new Double[]{(double)1234}));
        expectations.put("int_to_array<int>", Arrays.asList(new Integer[]{(int)1234}));
        expectations.put("int_to_array<timestamp>", null);
        expectations.put("int_to_array<short>", Arrays.asList(new Short[]{(short)1234}));
        expectations.put("int_to_array<long>", Arrays.asList(new Long[]{(long)1234}));

        expectations.put("arr_int_to_boolean", null);
        expectations.put("arr_int_to_byte", null);
        expectations.put("arr_int_to_short", null);
        expectations.put("arr_int_to_int", null);
        expectations.put("arr_int_to_long", null);
        expectations.put("arr_int_to_float",null);
        expectations.put("arr_int_to_double", null);
        expectations.put("arr_int_to_decimal", null);
        expectations.put("arr_int_to_string", "[1234,4321]");
        expectations.put("arr_int_to_date", null);
        expectations.put("arr_int_to_timestamp", null);
        expectations.put("arr_int_to_binary", null);
        expectations.put("arr_int_to_array<boolean>", null);
        expectations.put("arr_int_to_array<float>",  Arrays.asList(new Float[]{(float)1234, (float)4321}));
        expectations.put("arr_int_to_array<date>", null);
        expectations.put("arr_int_to_array<string>", Arrays.asList(new String[]{"1234", "4321"}));
        expectations.put("arr_int_to_array<byte>", Arrays.asList(new Byte[]{(byte) -46, (byte) -31}));
        expectations.put("arr_int_to_array<double>", Arrays.asList(new Double[]{(double)1234, (double)4321}));
        expectations.put("arr_int_to_array<decimal>", Arrays.asList(new Double[]{(double)1234, (double)4321}));
        expectations.put("arr_int_to_array<int>", Arrays.asList(new Integer[]{(int)1234, (int)4321}));
        expectations.put("arr_int_to_array<timestamp>", null);
        expectations.put("arr_int_to_array<short>", Arrays.asList(new Short[]{(short)1234, (short)4321}));
        expectations.put("arr_int_to_array<long>", Arrays.asList(new Long[]{(long)1234, (long)4321}));


        expectations.put("long_to_boolean", null);
        expectations.put("long_to_byte", (byte)-82);
        expectations.put("long_to_short", (short)14254);
        expectations.put("long_to_int", (int)-376621138);
        expectations.put("long_to_long", (long) 1653185787822L);
        expectations.put("long_to_float", (float) 1653185787822L);
        expectations.put("long_to_double", (double) 1653185787822L);
        expectations.put("long_to_decimal", (double) 1653185787822L);
        expectations.put("long_to_string", "1653185787822");
        expectations.put("long_to_date", null);
        expectations.put("long_to_timestamp", Instant.ofEpochMilli(1653185787822L));
        expectations.put("long_to_binary", null);
        expectations.put("long_to_array<boolean>", null);
        expectations.put("long_to_array<float>", Collections.singletonList((float) 1653185787822L));
        expectations.put("long_to_array<date>", null);
        expectations.put("long_to_array<string>", Collections.singletonList("1653185787822"));
        expectations.put("long_to_array<byte>", Collections.singletonList((byte)-82));
        expectations.put("long_to_array<double>", Collections.singletonList((double) 1653185787822L));
        expectations.put("long_to_array<decimal>", Collections.singletonList((double) 1653185787822L));
        expectations.put("long_to_array<int>", Collections.singletonList((int)-376621138));
        expectations.put("long_to_array<timestamp>", Collections.singletonList(Instant.ofEpochMilli(1653185787822L)));
        expectations.put("long_to_array<short>", Collections.singletonList( (short)14254));
        expectations.put("long_to_array<long>", Collections.singletonList((long) 1653185787822L));

        expectations.put("arr_long_to_boolean", null);
        expectations.put("arr_long_to_byte", null);
        expectations.put("arr_long_to_short", null);
        expectations.put("arr_long_to_int", null);
        expectations.put("arr_long_to_long", null);
        expectations.put("arr_long_to_float", null);
        expectations.put("arr_long_to_double", null);
        expectations.put("arr_long_to_decimal", null);
        expectations.put("arr_long_to_string", "[1653185787822,1653185787824]");
        expectations.put("arr_long_to_date", null);
        expectations.put("arr_long_to_timestamp",null);
        expectations.put("arr_long_to_binary", null);
        expectations.put("arr_long_to_array<boolean>", null);
        expectations.put("arr_long_to_array<float>", Arrays.asList(new Float[]{(float)1653185787822L,(float)1653185787824L}));
        expectations.put("arr_long_to_array<date>", null);
        expectations.put("arr_long_to_array<string>", Arrays.asList(new String[]{"1653185787822","1653185787824"}));
        expectations.put("arr_long_to_array<byte>", Arrays.asList(new Byte[]{(byte)1653185787822L,(byte)1653185787824L}));
        expectations.put("arr_long_to_array<double>", Arrays.asList(new Double[]{(double)1653185787822L,(double)1653185787824L}));
        expectations.put("arr_long_to_array<decimal>", Arrays.asList(new Double[]{(double)1653185787822L,(double)1653185787824L}));
        expectations.put("arr_long_to_array<int>", Arrays.asList(new Integer[]{(int)1653185787822L,(int)1653185787824L}));
        expectations.put("arr_long_to_array<timestamp>", Arrays.asList(new Instant[]{ Instant.ofEpochMilli(1653185787822L),  Instant.ofEpochMilli(1653185787824L)}));
        expectations.put("arr_long_to_array<short>", Arrays.asList(new Short[]{(short)1653185787822L,(short)1653185787824L}));
        expectations.put("arr_long_to_array<long>", Arrays.asList(new Long[]{(long)1653185787822L,(long)1653185787824L}));

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
        expectations.put("float_to_array<boolean>", null);
        expectations.put("float_to_array<float>", Collections.singletonList( (float) 1.234));
        expectations.put("float_to_array<date>", null);
        expectations.put("float_to_array<string>", Collections.singletonList("1.2343"));
        expectations.put("float_to_array<byte>", Collections.singletonList((byte) 1));
        expectations.put("float_to_array<double>", Collections.singletonList((double) 1.2343));
        expectations.put("float_to_array<decimal>", Collections.singletonList((double) 1.2343));
        expectations.put("float_to_array<int>", Collections.singletonList((int) 1));
        expectations.put("float_to_array<timestamp>", null);
        expectations.put("float_to_array<short>", Collections.singletonList((short) 1));
        expectations.put("float_to_array<long>", Collections.singletonList((long) 1));

        expectations.put("arr_float_to_boolean", null);
        expectations.put("arr_float_to_byte", null);
        expectations.put("arr_float_to_short", null);
        expectations.put("arr_float_to_int", null);
        expectations.put("arr_float_to_long", null);
        expectations.put("arr_float_to_float", null);
        expectations.put("arr_float_to_double", null);
        expectations.put("arr_float_to_decimal", null);
        expectations.put("arr_float_to_string", "[1.2343,1.3343]");
        expectations.put("arr_float_to_date", null);
        expectations.put("arr_float_to_timestamp", null);
        expectations.put("arr_float_to_binary", null);
        expectations.put("arr_float_to_array<boolean>", null);
        expectations.put("arr_float_to_array<float>", Arrays.asList(new Float[]{(float)1.2343, (float)1.3343}));
        expectations.put("arr_float_to_array<date>", null);
        expectations.put("arr_float_to_array<string>", Arrays.asList(new String[]{"1.2343", "1.3343"}));
        expectations.put("arr_float_to_array<byte>", Arrays.asList(new Byte[]{(byte)1.2343, (byte)1.3343}));
        expectations.put("arr_float_to_array<double>", Arrays.asList(new Double[]{(double)1.2343, (double)1.3343}));
        expectations.put("arr_float_to_array<decimal>", Arrays.asList(new Double[]{(double)1.2343, (double)1.3343}));
        expectations.put("arr_float_to_array<int>", Arrays.asList(new Integer[]{(int)1.2343, (int)1.3343}));
        expectations.put("arr_float_to_array<timestamp>", null);
        expectations.put("arr_float_to_array<short>", Arrays.asList(new Short[]{(short)1.2343, (short)1.3343}));
        expectations.put("arr_float_to_array<long>", Arrays.asList(new Long[]{(long)1.2343, (long)1.3343}));


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
        expectations.put("double_to_array<boolean>", null);
        expectations.put("double_to_array<float>", Collections.singletonList((float) 1.2345678));
        expectations.put("double_to_array<date>", null);
        expectations.put("double_to_array<string>", Collections.singletonList("1.23456789123"));
        expectations.put("double_to_array<byte>", Collections.singletonList((byte) 1L));
        expectations.put("double_to_array<double>", Collections.singletonList(1.23456789123));
        expectations.put("double_to_array<decimal>", Collections.singletonList(1.23456789123));
        expectations.put("double_to_array<int>", Collections.singletonList((int) 1));
        expectations.put("double_to_array<timestamp>", null);
        expectations.put("double_to_array<short>", Collections.singletonList((short) 1L));
        expectations.put("double_to_array<long>", Collections.singletonList((long) 1L));

        expectations.put("arr_double_to_boolean", null);
        expectations.put("arr_double_to_byte", null);
        expectations.put("arr_double_to_short", null);
        expectations.put("arr_double_to_int", null);
        expectations.put("arr_double_to_long", null);
        expectations.put("arr_double_to_float", null);
        expectations.put("arr_double_to_double", null);
        expectations.put("arr_double_to_decimal", null);
        expectations.put("arr_double_to_string", "[1.23456789123,1.33456789123]");
        expectations.put("arr_double_to_date", null);
        expectations.put("arr_double_to_timestamp",null);
        expectations.put("arr_double_to_binary", null);
        expectations.put("arr_double_to_array<boolean>", null);
        expectations.put("arr_double_to_array<float>", Arrays.asList(new Float[]{(float)1.23456789,(float)1.33456789}));
        expectations.put("arr_double_to_array<date>", null);
        expectations.put("arr_double_to_array<string>", Arrays.asList(new String[]{"1.23456789123","1.33456789123"}));
        expectations.put("arr_double_to_array<byte>", Arrays.asList(new Byte[]{(byte)1.23456789123,(byte)1.33456789123}));
        expectations.put("arr_double_to_array<double>", Arrays.asList(new Double[]{(double)1.23456789123,(double)1.33456789123}));
        expectations.put("arr_double_to_array<decimal>", Arrays.asList(new Double[]{(double)1.23456789123,(double)1.33456789123}));
        expectations.put("arr_double_to_array<int>", Arrays.asList(new Integer[]{(int)1.23456789123,(int)1.33456789123}));
        expectations.put("arr_double_to_array<timestamp>", null);
        expectations.put("arr_double_to_array<short>", Arrays.asList(new Short[]{(short)1.23456789123,(short)1.33456789123}));
        expectations.put("arr_double_to_array<long>", Arrays.asList(new Long[]{(long)1.23456789123,(long)1.33456789123}));

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
        expectations.put("date_to_array<boolean>", null);
        expectations.put("date_to_array<float>", null);
        expectations.put("date_to_array<date>", Collections.singletonList(LocalDate.parse("2022-03-04")));
        expectations.put("date_to_array<string>", Collections.singletonList("2022-03-04"));
        expectations.put("date_to_array<byte>", null);
        expectations.put("date_to_array<double>", null);
        expectations.put("date_to_array<decimal>", null);
        expectations.put("date_to_array<int>", null);
        expectations.put("date_to_array<timestamp>", null);
        expectations.put("date_to_array<short>", null);
        expectations.put("date_to_array<long>", null);

        expectations.put("arr_date_to_boolean", null);
        expectations.put("arr_date_to_byte", null);
        expectations.put("arr_date_to_short", null);
        expectations.put("arr_date_to_int", null);
        expectations.put("arr_date_to_long", null);
        expectations.put("arr_date_to_float", null);
        expectations.put("arr_date_to_double", null);
        expectations.put("arr_date_to_decimal", null);
        expectations.put("arr_date_to_string", "[\"2022-03-04\",\"2022-03-05\"]");
        expectations.put("arr_date_to_date", null);
        expectations.put("arr_date_to_timestamp", null);
        expectations.put("arr_date_to_binary", null);
        expectations.put("arr_date_to_array<boolean>", null);
        expectations.put("arr_date_to_array<float>", null);
        expectations.put("arr_date_to_array<date>", Arrays.asList(new LocalDate[]{LocalDate.parse("2022-03-04"), LocalDate.parse("2022-03-05")}));
        expectations.put("arr_date_to_array<string>", Arrays.asList(new String[]{"2022-03-04","2022-03-05"}));
        expectations.put("arr_date_to_array<byte>", null);
        expectations.put("arr_date_to_array<double>", null);
        expectations.put("arr_date_to_array<decimal>", null);
        expectations.put("arr_date_to_array<int>", null);
        expectations.put("arr_date_to_array<timestamp>", null);
        expectations.put("arr_date_to_array<short>", null);
        expectations.put("arr_date_to_array<long>", null);


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
        expectations.put("string_to_array<boolean>", null);
        expectations.put("string_to_array<float>", null);
        expectations.put("string_to_array<date>", null);
        expectations.put("string_to_array<string>", Collections.singletonList("cat"));
        expectations.put("string_to_array<byte>", null);
        expectations.put("string_to_array<double>", null);
        expectations.put("string_to_array<decimal>", null);
        expectations.put("string_to_array<int>", null);
        expectations.put("string_to_array<timestamp>", null);
        expectations.put("string_to_array<short>", null);
        expectations.put("string_to_array<long>", null);

        expectations.put("arr_string_to_boolean", null);
        expectations.put("arr_string_to_byte", null);
        expectations.put("arr_string_to_short", null);
        expectations.put("arr_string_to_int", null);
        expectations.put("arr_string_to_long", null);
        expectations.put("arr_string_to_float",null);
        expectations.put("arr_string_to_double", null);
        expectations.put("arr_string_to_decimal", null);
        expectations.put("arr_string_to_string",  "[\"a\",\"b\"]");
        expectations.put("arr_string_to_date", null);
        expectations.put("arr_string_to_timestamp", null);
        expectations.put("arr_string_to_binary", null);
        expectations.put("arr_string_to_array<boolean>", null);
        expectations.put("arr_string_to_array<float>", null);
        expectations.put("arr_string_to_array<date>", null);
        expectations.put("arr_string_to_array<string>", Arrays.asList(new String[]{"a","b"}));
        expectations.put("arr_string_to_array<byte>", null);
        expectations.put("arr_string_to_array<double>", null);
        expectations.put("arr_string_to_array<decimal>", null);
        expectations.put("arr_string_to_array<int>", null);
        expectations.put("arr_string_to_array<timestamp>", null);
        expectations.put("arr_string_to_array<short>", null);
        expectations.put("arr_string_to_array<long>", null);

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
        expectations.put("boolean_to_array<boolean>", Collections.singletonList(false));
        expectations.put("boolean_to_array<float>", null);
        expectations.put("boolean_to_array<date>", null);
        expectations.put("boolean_to_array<string>", Collections.singletonList("false"));
        expectations.put("boolean_to_array<byte>", null);
        expectations.put("boolean_to_array<double>", null);
        expectations.put("boolean_to_array<decimal>", null);
        expectations.put("boolean_to_array<int>", null);
        expectations.put("boolean_to_array<timestamp>", null);
        expectations.put("boolean_to_array<short>", null);
        expectations.put("boolean_to_array<long>", null);


        TypeDescription td = TypeDescription.createStruct();
        td = td.addField("id", TypeDescription.createString());
        td = td.addField("date", TypeDescription.createDate());

        String jsonObjectStr = "{\"id\":\"my_id\",";
        for (String jsonTypeName : jsonTypes.keySet()) {
            for (String orcTypeName : orcTypes.keySet()) {
                String columnName = jsonTypeName + "_to_" + orcTypeName;
                if(orcTypeName.startsWith("array<")) {
                    td = td.addField(columnName, TypeDescription.createList((TypeDescription) TypeDescription.class.getMethod(orcTypes.get(orcTypeName.substring(6, orcTypeName.length()-1))).invoke(TypeDescription.class)));
                }else {
                    td = td.addField(columnName, (TypeDescription) TypeDescription.class.getMethod(orcTypes.get(orcTypeName)).invoke(TypeDescription.class));
                }
             //   td = td.addField(columnName, TypeDescription.fromString (orcTypeName));// TypeDescription.class.getMethod(orcTypes.get(orcTypeName)).invoke(TypeDescription.class));
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
        Reader reader = OrcFile.createReader(new Path(basePath,filename), OrcFile.readerOptions(new Configuration()));
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

                        }  else if (orcTypeName.equals("array<date>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof LocalDate) {
                                            assertTrue(new Long(((LongColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)]).equals(((LocalDate) expectedList.get(i)).toEpochDay()));
                                        }else if(expectedList.get(i) instanceof String) {
                                            assertTrue(new String(((BytesColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)]).equals(expectedList.get(i)));
                                        }else {
                                            assertTrue(false);
                                        }

                                    }
                                }

                              //  assertTrue((((Boolean)expected) ? 1 : 0) == orcCol.vector[rowNum]);
                            } else {
                                assertTrue(false);
                            }

                        } else if (orcTypeName.equals("array<string>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                         if(expectedList.get(i) instanceof String) {
                               BytesColumnVector vec = ((BytesColumnVector) orcCol.child);
                               String str;
                               if(!vec.isRepeating)
                                             str= new String(vec.vector[rowNum+i], vec.start[rowNum+i], vec.length[rowNum+i]);
                               else{
                                   str= new String(vec.vector[0], vec.start[0], vec.length[0]);
                               }
                                            assertTrue(str.equals(expectedList.get(i)));
                                        }else {
                                             assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<double>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Double) {
                                            assertTrue(((DoubleColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)] == ((Double)expectedList.get(i)).doubleValue());
                                        }else {
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<float>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Float) {
                                            assertTrue(expectedList.get(i).toString().substring(0, Math.min(expectedList.get(i).toString().length(), 8)).equals(new Double(((DoubleColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)]).toString().substring(0, Math.min(expectedList.get(i).toString().length(), 8))));

                                            }else {
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<byte>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Byte) {
                                            assertTrue(((LongColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)] == ((Byte)expectedList.get(i)).byteValue());
                                        }else {
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<decimal>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Double) {
                                            assertTrue(((DecimalColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)].getHiveDecimal().doubleValue() == ((Double)expectedList.get(i)).doubleValue());
                                        }else {
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<int>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Integer) {
                                            assertTrue(((LongColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)] == ((Integer)expectedList.get(i)).intValue());
                                        }else {
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<short>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Short) {
                                            assertTrue(((LongColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)] == ((Short)expectedList.get(i)).intValue());
                                        }else {
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<long>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Long) {
                                            assertTrue(((LongColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)] == ((Long)expectedList.get(i)).longValue());
                                        }else {
                                            System.out.println("ASD");
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<timestamp>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Instant) {
                                              assertTrue(((TimestampColumnVector) orcCol.child).time[(int) (orcCol.offsets[rowNum] + i)]  / 1000 == ((Instant) expectedList.get(i)).toEpochMilli() / 1000);

                                        }else {
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else if (orcTypeName.equals("array<boolean>")) {
                            ListColumnVector orcCol = (ListColumnVector) batch.cols[idx];

                            if (expected instanceof List) {
                                List<Object> expectedList = (List<Object>) expected;
                                assertTrue(orcCol.lengths[rowNum]==expectedList.size());
                                if(expectedList.size()>0) {
                                    for (int i = 0; i < expectedList.size(); i++) {
                                        if(expectedList.get(i) instanceof Boolean) {
                                            assertTrue(((LongColumnVector) orcCol.child).vector[(int) (orcCol.offsets[rowNum] + i)] == (((Boolean)expectedList.get(i)).booleanValue() ? 1 : 0));

                                        }else {
                                            assertTrue(false);
                                        }

                                    }
                                }
                            } else {
                                assertTrue(false);
                            }

                        }else {
                            System.out.println("ASD");
                        }
                    }
                }

                System.out.println("Row: " + dateCol.vector[rowNum] + " " + idCol.vector[rowNum].toString());
            }
        }
    }
}
