package com.mcneilio.shokuyoku.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.json.JSONObject;
import org.junit.Test;

public class JSONColumnFormatTest {

    @Test
    public void flattenPerformance() throws Exception {
        String path = "src/test/resources";
        File file = new File(path);
        String resourcesPath = file.getAbsolutePath() + "/testEvent.json";

        String eventText = new String(Files.readAllBytes(Paths.get(resourcesPath)));
        JSONObject eventJson = new JSONObject(eventText);
        long result = 0;
        for(int i=0; i<10; i++) {
            long timer = System.nanoTime();
            new JSONColumnFormat(eventJson).getFlattened();
            result += System.nanoTime() - timer;
        }
        long finalResult = result/10;
        System.out.println("JSON Flattened 10 times taking an average of: " + finalResult + " ns");
        assert finalResult < 1000000;
    }

    @Test
    public void flattenDoesNotContainTopLevelObjects() throws Exception {
        JSONObject eventMsg = getTestJSON();

        assertThat(eventMsg.has("context")).isFalse();
        assertThat(eventMsg.has("integrations")).isFalse();
        assertThat(eventMsg.has("properties")).isFalse();
        assertThat(eventMsg.has("_metadata")).isFalse();
    }

    @Test
    public void flattenMergesTopLevelObjects() throws Exception {
        JSONObject eventMsg = getTestJSON();

        assertThat(eventMsg.has("context_automation")).isTrue();
        assertThat(eventMsg.has("integrations_google_analytics")).isTrue();
        assertThat(eventMsg.has("properties_domain")).isTrue();
        assertThat(eventMsg.has("_metadata_bundled")).isTrue();
    }

    @Test
    public void flattenConvertCamelCaseToSnakeCase() throws Exception {
        JSONObject eventMsg = getTestJSON();

        assertThat(eventMsg.has("messageid")).isFalse();
        assertThat(eventMsg.has("message_id")).isTrue();
    }

    @Test
    public void flattenConvertCamelCaseToSnakeCaseNested() throws Exception {
        JSONObject eventMsg = getTestJSON();

        assertThat(eventMsg.has("context_user_agent")).isTrue();
    }

    @Test
    public void flattenConvertDotName() throws Exception {
        JSONObject eventMsg = getTestJSON();

        assertThat(eventMsg.has("properties_test_dot_char")).isTrue();
    }

    private JSONObject getTestJSON() throws Exception {
        String path = "src/test/resources";
        File file = new File(path);
        String resourcesPath = file.getAbsolutePath() + "/testEvent.json";

        String eventText = new String(Files.readAllBytes(Paths.get(resourcesPath)));
        JSONObject eventJson = new JSONObject(eventText);
        return new JSONColumnFormat(eventJson).getFlattened();
    }
}
