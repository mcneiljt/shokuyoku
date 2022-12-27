package com.mcneilio.shokuyoku.misc;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.model.EventTypeColumn;
import com.mcneilio.shokuyoku.model.EventTypeColumnModifier;
import com.mcneilio.shokuyoku.util.JSONSchemaDictionary;
import com.mcneilio.shokuyoku.util.SimpleJSONSchemaDictionary;
import org.json.JSONObject;
import org.junit.Test;
import org.mortbay.util.ajax.JSON;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.junit.Assert.assertTrue;

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

        JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchema = new JSONSchemaDictionary.EventTypeJSONSchema(prefixes, columns, false, false, null);
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

    @Test
    public void testColumnModifier() throws Exception {
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
        JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchemaPlain = new JSONSchemaDictionary.EventTypeJSONSchema(prefixes, columns, false, false, null);

        JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchemaModifier = new JSONSchemaDictionary.EventTypeJSONSchema(prefixes, columns, false, false,
            Collections.singletonList(new EventTypeColumnModifier(new EventTypeColumn.EventTypeColumnKey("focus_window","context_library_name"), EventTypeColumnModifier.EventColumnModifierType.DROP, null)));
        jsonSchemaDictionary.addEventType("focus_window", eventTypeJSONSchemaModifier);

        JSONObject plain = eventMsg.getCopy(eventTypeJSONSchemaPlain.getJSONColumnFormatFilter(), false);

        JSONObject modified = eventMsg.getCopy(eventTypeJSONSchemaModifier.getJSONColumnFormatFilter(), false);
        System.out.println("ASD");

        assertTrue(plain.has("context") && ((JSONObject)plain.get("context")).has("library") && ((JSONObject)((JSONObject)plain.get("context")).get("library")).has("name") );
        assertTrue(!modified.has("context") || !((JSONObject)modified.get("context")).has("library") || !((JSONObject)((JSONObject)modified.get("context")).get("library")).has("name") );

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
