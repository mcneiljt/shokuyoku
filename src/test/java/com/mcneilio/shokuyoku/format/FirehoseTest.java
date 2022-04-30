package com.mcneilio.shokuyoku.format;

import static org.assertj.core.api.Assertions.assertThat;

import com.mcneilio.shokuyoku.format.Firehose;
import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FirehoseTest {

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    //@SetEnvironmentVariable(key = "ENDIAN", value = "little")
    @Test
    public void deserializePerformance() throws Exception {
        environmentVariables.set("ENDIAN", "big");
        String path = "src/test/resources";
        File file = new File(path);
        String resourcesPath = file.getAbsolutePath() + "/testEvent.json";

        String eventText = new String(Files.readAllBytes(Paths.get(resourcesPath)));
        Firehose test = new Firehose("testEvent", eventText);
        byte[] arry = test.getByteArray();
        long result = 0;
        for(int i=0; i<10; i++) {
            long timer = System.nanoTime();
            new Firehose(arry, false);
            result += System.nanoTime() - timer;
        }
        long finalResult = result/10;
        System.out.println("10 Firehose deserializations performed taking an average of: " + finalResult + " ns");
        assert finalResult < 1000000;
    }

    @Test
    public void serializePerformance() throws Exception {
        environmentVariables.set("ENDIAN", "big");
        String path = "src/test/resources";
        File file = new File(path);
        String resourcesPath = file.getAbsolutePath() + "/testEvent.json";

        String eventText = new String(Files.readAllBytes(Paths.get(resourcesPath)));
        long result = 0;
        for(int i=0; i<10; i++) {
            long timer = System.nanoTime();
            new Firehose("testEvent", eventText);
            result += System.nanoTime() - timer;
        }
        long finalResult = result/10;
        System.out.println("10 Firehose serializations performed taking an average of: " + finalResult + " ns");
        assert finalResult < 1000000;
    }

    @Test
    public void firehoseCanSerializeAndDeserializeLittleEndian() throws Exception {
        String testTopic = "testTopic";
        JSONObject testMessage = new JSONObject();

        testMessage.put("name", "testMessage");
        environmentVariables.set("ENDIAN", "little");
        environmentVariables.setup();
        Firehose firehoseMessage = new Firehose(testTopic, testMessage.toString());
        Firehose decodedMessage = new Firehose(firehoseMessage.getByteArray(), false);

        assertThat(decodedMessage.getTopic()).isEqualTo("test_topic");
    }
}
