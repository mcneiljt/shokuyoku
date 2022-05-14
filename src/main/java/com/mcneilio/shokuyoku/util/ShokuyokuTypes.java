package com.mcneilio.shokuyoku.util;

import org.apache.orc.TypeDescription;

import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;

public class ShokuyokuTypes {

    public static String[] getSupportedTypeStrings() {
        return new String[] {"array<","tinyint", "smallint", "int","bigint","float","double", "decimal", "string", "timestamp",  "date", "varchar", "boolean"};
    }

    public static Class getOrcJsonType(String orcType) {
        if (orcType.startsWith("array<")) {
            String tmp = orcType.substring(6, orcType.length() - 1);
            return Array.newInstance(getOrcJsonType(tmp), 0).getClass();
        } else if (orcType.equals("tinyint") || orcType.equals("smallint") || orcType.equals("int")) {
            return Double.class;
        } else if (orcType.equals("bigint")) {
            return Double.class;
        }

        // decimal types
        else if (orcType.equals("float") || orcType.equals("double") || orcType.startsWith("decimal(")) {
            return Double.class;
        }

        // string types
        else if (orcType.equals("string") || orcType.equals("timestamp") || orcType.equals("date")) {
            return String.class;
        } else if (orcType.startsWith("varchar(")) {
            return String.class;
        } else if (orcType.equals("boolean")) {
            return Boolean.class;
        } else {
            System.out.println("Unsupported Column Type: " + orcType);
            return null;
        }
    }

    public static String getOrcStringForClass(Class type){
        if (type.equals(Double.class)){
            return "double";
        } else if (type.equals(String.class)){
            return "string";
        } else if (type.equals(Integer.class)){
            return "int";
        }   else {
            System.out.println("Unsupported Column Type: " + type);
            return null;
        }
    }

    public static HashSet getCompatibleTypes(Class type, boolean allowInvalidCoercions){
        if (type.equals(Double.class)){
            HashSet<Class> allowedTypes = new HashSet(Arrays.asList(new Class[] {Integer.class, Long.class, Double.class, BigDecimal.class}));
            if (allowInvalidCoercions) {
                allowedTypes.addAll(Arrays.asList(new Class[] {String[].class, Integer[].class, BigDecimal[].class, Double[].class, Boolean[].class, Array.class}));
            }
            return allowedTypes;
        } else if(type.equals(Boolean.class)){
            return new HashSet(Arrays.asList(new Class[] {Boolean.class}));
        } else if(type.equals(String.class)){
            return new HashSet(Arrays.asList(new Class[] {String.class, Integer.class,Long.class, BigDecimal.class, Double.class, Boolean.class, String[].class, Integer[].class, BigDecimal[].class, Double[].class, Boolean[].class, Array.class}));
        } else if(type.equals(String[].class)) {
            return new HashSet(Arrays.asList(new Class[] {String[].class, Integer[].class, Long[].class, BigDecimal[].class, Double[].class, Boolean[].class, Array.class, String.class, Integer.class, Long.class, BigDecimal.class, Double.class, Boolean.class}));
        } else if(type.equals(Double[].class)) {
           return new HashSet(Arrays.asList(new Class[] {Integer[].class, BigDecimal[].class, Double[].class, Long[].class, Array.class}));
        } else {
            System.out.println("Issue");
        }
        return null;
    }

}
