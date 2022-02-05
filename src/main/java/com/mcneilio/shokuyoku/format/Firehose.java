package com.mcneilio.shokuyoku.format;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Originally designed by @thecubed who authored this spec
 This file defines the Firehose format for messages received on the Kafka FIREHOSE topic

 Firehose message definition
 ============================

 +-------+----------+--------+----------------+-----------+--------------+
 | magic | topicLen | msgLen |     topic      | separator |     msg      |
 +-------+----------+--------+----------------+-----------+--------------+
 | 0x01  | uint8    | uint32 | char[topicLen] | 0x02      | char[msgLen] |
 +-------+----------+--------+----------------+-----------+--------------+

 Every firehose object begins with the magic char 0x01 (ASCII start-of-heading)
 Topic length is expressed as a 8-bit unsigned integer (0x00 - 0xFF)
 Topics are a sequence of characters with length defined previously in topicLen
 Message length is expressed as a 32-bit unsigned integer
 The wrapped message is prefixed by a separator (0x02)

 A wrapped message should contain a single JSON object, with no trailing newline characters.

 Firehose objects are lightly compatible with streams, however they should usually be wrapped in a container such
 as a Kafka message, or stored in an array to provide seekability.

 Thanks for a little help from @IllegalArgument :)
 */

public class Firehose {

    public Firehose(String topic, String message) {
        this.topic = topic;
        this.message = message;
        byte[] topicBytes = topic.getBytes();
        byte[] messageBytes = message.getBytes();
        byte[] prefix = ByteBuffer.allocate(1).put((byte)0x1).array();
        byte[] topicLen = ByteBuffer.allocate(1).put((byte)topicBytes.length).array();
        byte[] messageLen = ByteBuffer.allocate(4).putInt(messageBytes.length).array();
        byte[] separator = ByteBuffer.allocate(1).put((byte)0x2).array();


        // Seems like this needs to also take endian into account
        ByteBuffer buffer = ByteBuffer.allocate(
                prefix.length + topicLen.length + messageLen.length +
                        separator.length + topicBytes.length + messageBytes.length
        );
        buffer.put(prefix);
        buffer.put(topicLen);
        buffer.put(messageLen);
        buffer.put(topicBytes);
        buffer.put(separator);
        buffer.put(messageBytes);

        this.byteArray = buffer.array();
    }

    public Firehose(byte[] byteArray) {
        this.byteArray = byteArray;


        int topicLength = byteArray[1] & 0xff;
        int msgLength;

        if (System.getenv("ENDIAN").equals("little")) // this should have a default
            msgLength = ((0xFF & byteArray[5]) << 24) | ((0xFF & byteArray[4]) << 16) |
                    ((0xFF & byteArray[3]) << 8) | (0xFF & byteArray[2]);
        else
            msgLength = ((0xFF & byteArray[2]) << 24) | ((0xFF & byteArray[3]) << 16) |
                    ((0xFF & byteArray[4]) << 8) | (0xFF & byteArray[5]);

        String fullTopic = new String(Arrays.copyOfRange(byteArray, 6, 6 +
                topicLength));

        // convert to snake case
        this.topic = fullTopic.replaceAll("([a-z])([A-Z]+)", "$1_$2").toLowerCase();

        this.message = new String(Arrays.copyOfRange(byteArray, 7 +
                topicLength, 7+topicLength+msgLength));
    }

    public byte[] getByteArray() {
        return byteArray;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessage() {
        return message;
    }

    private byte[] byteArray;
    private String topic, message;
}
