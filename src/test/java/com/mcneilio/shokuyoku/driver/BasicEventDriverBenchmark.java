package com.mcneilio.shokuyoku.driver;

import com.mcneilio.shokuyoku.helpers.SimpleTypeDescriptionProvider;
import org.json.JSONObject;
import org.junit.Test;

import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class BasicEventDriverBenchmark {

    @Test
    public void originalTest() throws Exception {
        String basePath = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
        LocalStorageDriver localStorageDriver = new LocalStorageDriver(basePath);

        SimpleTypeDescriptionProvider simpleTypeDescriptionProvider = new SimpleTypeDescriptionProvider();
        Map<String, String> columns = new HashMap<>();
        columns.put("id", "string");
        columns.put("count", "bigint");
        columns.put("date", "date");
        simpleTypeDescriptionProvider.addTypeDescription("event_name", columns);


        BasicEventDriver basicEventDriverNew = new BasicEventDriver("event_name", "2022-01-01", simpleTypeDescriptionProvider.getInstance("test", "event_name"), localStorageDriver);
        BasicEventDriverOld basicEventDriverOld = new BasicEventDriverOld("event_name", "2022-01-01", simpleTypeDescriptionProvider.getInstance("test", "event_name"), localStorageDriver);

        Properties props = new Properties();
        props.setProperty("storage.driver", "local");
        props.setProperty("storage.local.path", basePath);
        props.setProperty("statsd.prefix", "");
        props.setProperty("statsd.host", "localhost");
        props.setProperty("statsd.port", "8125");
        props.setProperty("statsd.flush.ms", "1000");
        props.setProperty("statsd.env", "");
        props.setProperty("event.date", "2022-01-01");
        props.setProperty("event.name", "event_name");
        com.mcneilio.orcmaker.BasicEventDriver basicEventDriverOrcMaker = new com.mcneilio.orcmaker.BasicEventDriver(props, simpleTypeDescriptionProvider.getInstance("test", "event_name"));




        runEventDriver(basicEventDriverNew, 10000000, "New");
        runEventDriver(basicEventDriverOld, 10000000, "Old");
        runEventDriver(basicEventDriverOrcMaker, 10000000, "OrcMaker");

        runEventDriver(basicEventDriverNew, 10000000, "New");
        runEventDriver(basicEventDriverOld, 10000000, "Old");
        runEventDriver(basicEventDriverOrcMaker, 10000000, "OrcMaker");

    }

    private static void runEventDriver(com.mcneilio.orcmaker.EventDriver basicEventDriver, int count, String prefix) {
        long startMS = System.currentTimeMillis();
        for(int i=0;i<count; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("id", "A"+Math.random());
            jsonObject.put("count", (int)Math.floor(Math.random()*10000));
            basicEventDriver.addMessage(jsonObject);
        }
        basicEventDriver.flush(true);

        System.out.println(prefix+" Total Time: "+(System.currentTimeMillis()-startMS));
    }
}
