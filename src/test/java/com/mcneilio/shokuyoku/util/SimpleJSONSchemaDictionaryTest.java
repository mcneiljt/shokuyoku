package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.format.Firehose;
import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.model.EventTypeColumn;
import com.mcneilio.shokuyoku.model.EventTypeColumnModifier;
import com.mcneilio.shokuyoku.util.JSONSchemaDictionary;
import com.mcneilio.shokuyoku.util.SimpleJSONSchemaDictionary;
import org.json.JSONObject;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleJSONSchemaDictionaryTest {

    @Test
    public void firehoseCanSerializeAndDeserializeLittleEndian() {
        JSONSchemaDictionary jsonSchemaDictionary = getTestJSON();

        assertThat(jsonSchemaDictionary.getEventJSONSchema("test_event_plain").hasColumn("context_field_one", "ASD")).isEqualTo(new Boolean(true));
        assertThat(jsonSchemaDictionary.getEventJSONSchema("test_event_plain").hasColumn("context_field_one", 1)).isEqualTo(new Boolean(true));

        assertThat(jsonSchemaDictionary.getEventJSONSchema("test_event_modified").hasColumn("context_field_one", 1)).isEqualTo(new Boolean(false));

    }

    private JSONSchemaDictionary getTestJSON()  {
        SimpleJSONSchemaDictionary simpleJSONSchemaDictionary = new SimpleJSONSchemaDictionary();
        Set<String> prefixes = new HashSet<>();
        Map<String, Class> fields = new HashMap<>();

        prefixes.add("context");
        fields.put("context_field_one", String.class);


        JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchemaOne = new JSONSchemaDictionary.EventTypeJSONSchema(prefixes, fields, false, false, null);
        JSONSchemaDictionary.EventTypeJSONSchema eventTypeJSONSchemaTwo = new JSONSchemaDictionary.EventTypeJSONSchema(prefixes, fields, false, false,
            Collections.singletonList(new EventTypeColumnModifier(new EventTypeColumn.EventTypeColumnKey("test_Event_modified", "context_field_one"), EventTypeColumnModifier.EventColumnModifierType.DROP, null))
            );

        simpleJSONSchemaDictionary.addEventType("test_event_plain", eventTypeJSONSchemaOne);
        simpleJSONSchemaDictionary.addEventType("test_event_modified", eventTypeJSONSchemaTwo);

        return simpleJSONSchemaDictionary;
    }
}
