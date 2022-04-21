package com.mcneilio;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.util.JSONSchemaDictionary;
import com.mcneilio.shokuyoku.util.SimpleJSONSchemaDictionary;
import org.json.JSONObject;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class JSONColumnFormatFilterTest {

    @Test
    public void flattenDoesNotContainTopLevelObjects() throws Exception {
        JSONColumnFormat eventMsg = getTestJSON();

        SimpleJSONSchemaDictionary jsonSchemaDictionary = new SimpleJSONSchemaDictionary();

        Set<String> prefixes = new HashSet<>();
        Map<String, Class> columns = new HashMap<>();
        prefixes.add("context");
        prefixes.add("context_library");
        prefixes.add("page_path");
        prefixes.add("test");
        prefixes.add("test_dot");
        prefixes.add("_metadata");

        columns.put("context_library_name", String.class);
        columns.put("page_path", String.class);
        columns.put("_metadata_bundled", String[].class);

        columns.put("test_dot_char", String.class);

        JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchema = new JSONSchemaDictionary.EventTypeJSONSchema(prefixes, columns, false, false);
        jsonSchemaDictionary.addEventType("focus_window", eventTypeJSONSchema);

        JSONObject filtered = eventMsg.getCopy(eventTypeJSONSchema.getJSONColumnFormatFilter(), false);
        JSONObject filtered3 = eventMsg.getCopy(eventTypeJSONSchema.getJSONColumnFormatFilter(), false, Collections.singleton("properties"));

        JSONObject filtered2 = eventMsg.getCopy(eventTypeJSONSchema.getJSONColumnFormatFilter(), true, Collections.singleton("properties"));

        System.out.println("ASD");
        //        assertThat(eventMsg.has("context")).isFalse();
        //        assertThat(eventMsg.has("integrations")).isFalse();
        //        assertThat(eventMsg.has("properties")).isFalse();
        //        assertThat(eventMsg.has("_metadata")).isFalse();
    }

    private JSONColumnFormat getTestJSON() throws Exception {
        String path = "src/test/resources";
        File file = new File(path);
        String resourcesPath = file.getAbsolutePath() + "/testEvent.json";

        String eventText = new String(Files.readAllBytes(Paths.get(resourcesPath)));
        JSONObject eventJson = new JSONObject(eventText);
        return new JSONColumnFormat(eventJson);
    }
}
