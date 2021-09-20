package com.mcneilio.shokuyoku.driver;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;

import java.util.ArrayList;

public interface EventDriver {

    /**
     * This should add a single message to be written to the event store
     */
    void addMessage(String msg);

    /**
     * This should write all messages to the event store and reset all buffers
     */
    void flush();
}
