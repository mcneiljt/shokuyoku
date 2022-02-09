package com.mcneilio.shokuyoku.driver;

import org.json.JSONObject;

public interface EventDriver {

    /**
     * This should add a single message to be written to the event store
     */
    void addMessage(JSONObject msg);

    /**
     * This should write all messages to the event store and reset all buffers
     * @return
     */
    String flush(boolean deleteFile);
}
