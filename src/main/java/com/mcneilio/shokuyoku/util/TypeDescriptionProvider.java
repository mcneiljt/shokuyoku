package com.mcneilio.shokuyoku.util;


import org.apache.orc.TypeDescription;


public interface TypeDescriptionProvider {
    public  TypeDescription getInstance(String databaseName, String eventName) ;
}
