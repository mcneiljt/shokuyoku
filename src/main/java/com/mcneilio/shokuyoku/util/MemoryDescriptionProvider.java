package com.mcneilio.shokuyoku.util;

import org.apache.orc.TypeDescription;

import java.util.HashMap;
import java.util.Map;

public class MemoryDescriptionProvider implements TypeDescriptionProvider {

    private static Map<String, TypeDescription> typeDescriptions = null;

    public MemoryDescriptionProvider() {
        typeDescriptions = new HashMap<>();
    }


    public  void setinstance(String eventName, TypeDescription typeDescription){
        if(typeDescriptions==null){
            typeDescriptions=new HashMap<>();
        }
        typeDescriptions.put(eventName, typeDescription);
    }

    @Override
    public TypeDescription getInstance(String databaseName, String eventName) throws Exception {
        return typeDescriptions.get(eventName);
    }
}
