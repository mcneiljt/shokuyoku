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
        // warning this is signed and the spec expects unsigned but it isn't often used
        byte[] messageLen = ByteBuffer.allocate(4).putInt(messageBytes.length).array();;
        byte[] separator = ByteBuffer.allocate(1).put((byte)0x2).array();

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
        this.topic = new String(Arrays.copyOfRange(byteArray, 6, 6 +
                (byteArray[1] & 0xf)));
        this.message = new String(Arrays.copyOfRange(byteArray, 7 +
                (byteArray[1] & 0xf), byteArray.length));
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
