package com.mcneilio.shokuyoku.driver;

import com.mcneilio.shokuyoku.helpers.SimpleTypeDescriptionProvider;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class BasicEventDriverTest {
    @Test
    public void firehoseCanSerializeAndDeserializeLittleEndian() throws IOException {
        String basePath = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
        LocalStorageDriver localStorageDriver = new LocalStorageDriver(basePath);

        SimpleTypeDescriptionProvider simpleTypeDescriptionProvider = new SimpleTypeDescriptionProvider();
        Map<String,String> columns = new HashMap<>();
        columns.put("count", "bigint");
        columns.put("date", "date");
        simpleTypeDescriptionProvider.addTypeDescription("event_name", columns);

        BasicEventDriver basicEventDriver = new BasicEventDriver( "event_name", "2022-01-01", simpleTypeDescriptionProvider.getInstance("test", "event_name"), localStorageDriver);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("count", 1);
        basicEventDriver.addMessage(jsonObject);

        basicEventDriver.flush(true);

        System.out.println("ASD");
    }
}
