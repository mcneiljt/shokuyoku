package com.mcneilio.shokuyoku.util;

import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.lang.reflect.GenericDeclaration;
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

    public static Class getArrayType(JSONArray jsonArray) {
        if(jsonArray.length()==0){
            return Array.class;
        }
        Class c = jsonArray.get(0).getClass();
        for(int i=1;i<jsonArray.length();i++){
            if(c.equals(String.class) || c.equals(JSONObject.class)){
                break;
            }
            Class newC = jsonArray.get(i).getClass();
            if(!c.equals(newC)){
                return String.class;
            }
        }

        if(c.equals(JSONObject.class)) {
            return String.class;
        }

        return Array.newInstance(c, 0).getClass();
    }

    public static String getOrcStringForClass(Class type){
        if (type.equals(Double.class) || type.equals(BigDecimal.class)){
            return "double";
        } else if (type.equals(String.class)){
            return "string";
        } else if (type.equals(Boolean.class)){
            return "boolean";
        }  else if (type.equals(Integer.class)){
            return "int";
        } else if (type.equals(Integer[].class)){
            return "array<int>";
        } else if (type.equals(String[].class)){
            return "array<string>";
        }  else {
            System.out.println("Unsupported Column Type1: " + type);
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
