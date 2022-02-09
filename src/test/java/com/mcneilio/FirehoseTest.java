package com.mcneilio;

import static org.assertj.core.api.Assertions.assertThat;

import com.mcneilio.shokuyoku.format.Firehose;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;

public class FirehoseTest {

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    //@SetEnvironmentVariable(key = "ENDIAN", value = "little")
    @Test
    public void firehoseCanSerializeAndDeserializeLittleEndian() throws Exception {
        String testTopic = "testTopic";
        JSONObject testMessage = new JSONObject();

        testMessage.put("name", "testMessage");
        environmentVariables.set("ENDIAN", "little");
        environmentVariables.setup();
        Firehose firehoseMessage = new Firehose(testTopic, testMessage.toString());
        Firehose decodedMessage = new Firehose(firehoseMessage.getByteArray());

        assertThat(decodedMessage.getTopic()).isEqualTo("test_topic");
    }
}
