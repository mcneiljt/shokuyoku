package com.mcneilio.shokuyoku.misc;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.util.JSONSchemaDictionary;
import com.mcneilio.shokuyoku.util.SimpleJSONSchemaDictionary;
import org.json.JSONObject;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class JSONColumnFormatFilterBenchmark {
    @Test
    public void flattenDoesNotContainTopLevelObjects() throws Exception {
        SimpleJSONSchemaDictionary jsonSchemaDictionary = new SimpleJSONSchemaDictionary();

        Set<String> prefixes = new HashSet<>();
        Map<String, Class> columns = new HashMap<>();
        prefixes.add("context");
        prefixes.add("context_library");
        prefixes.add("page_path");

        columns.put("context_library_name", String.class);
        columns.put("page_path", String.class);

        JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchema = new JSONSchemaDictionary.EventTypeJSONSchema(prefixes, columns, false, false);
        jsonSchemaDictionary.addEventType("focus_window", eventTypeJSONSchema);

        String eventText = getTestJSONStr();

        long startTime = System.currentTimeMillis();
        for(int i=0; i<100000;i++){
            JSONObject eventJson = new JSONObject(eventText);
            JSONColumnFormat jsonColumnFormat = new JSONColumnFormat(eventJson);
            jsonColumnFormat.getCopy(eventTypeJSONSchema.getJSONColumnFormatFilter(), true, Collections.singleton("properties"));
           // return new JSONColumnFormat(eventJson);
        }
        System.out.println("Total Time: "+(System.currentTimeMillis()-startTime));
    }

    private String getTestJSONStr() throws Exception {
        String path = "src/test/resources";
        File file = new File(path);
        String resourcesPath = file.getAbsolutePath() + "/testEvent.json";

        String eventText = new String(Files.readAllBytes(Paths.get(resourcesPath)));
        return eventText;
    }
}
