package com.mcneilio.shokuyoku.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.mcneilio.shokuyoku.common.Firehose;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class FirehoseTest {

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    @Test
    public void firehoseCanSerializeAndDeserializeLittleEndian() {
        String testTopic = "testTopic";
        JSONObject testMessage = new JSONObject();

        testMessage.put("name", "testMessage");
        environmentVariables.set("ENDIAN", "little");
        Firehose firehoseMessage = new Firehose(testTopic, testMessage.toString());
        Firehose decodedMessage = new Firehose(firehoseMessage.getByteArray());

        assertThat(decodedMessage.getTopic()).isEqualTo("test_topic");
    }
}
