package com.mcneilio.shokuyoku.helpers;

import com.mcneilio.shokuyoku.util.MemoryDescriptionProvider;
import org.apache.orc.TypeDescription;

import java.util.Map;

import static com.mcneilio.shokuyoku.util.HiveDescriptionProvider.getTypeDescriptionByString;

public class SimpleTypeDescriptionProvider extends MemoryDescriptionProvider {

    public void addTypeDescription(String eventName, Map<String, String> columns ){
        TypeDescription td=  TypeDescription.createStruct();
        for (String column: columns.keySet()){
            TypeDescription type = getTypeDescriptionByString(columns.get(column));
            td.addField(column, type);
        }

        setinstance(eventName, td);
    }
}
