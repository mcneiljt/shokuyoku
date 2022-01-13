package com.mcneilio;

import static org.assertj.core.api.Assertions.assertThat;

import com.mcneilio.shokuyoku.format.Firehose;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class FirehoseTest {

    JSONObject testMessage = new JSONObject();

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void firehoseCanSerializeAndDeserializeLittleEndian() {
        String testTopic = "testTopic";
        testMessage.put("name", "testMessage");
        environmentVariables.set("ENDIAN", "little");
        Firehose firehoseMessage = new Firehose(testTopic, testMessage.toString());
        Firehose decodedMessage = new Firehose(firehoseMessage.getByteArray());

        assertThat(decodedMessage.getTopic()).isEqualTo("test_topic");
    }
}
