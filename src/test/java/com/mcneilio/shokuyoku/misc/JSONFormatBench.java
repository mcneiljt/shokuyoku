package com.mcneilio.shokuyoku.misc;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class JSONFormatBench {

    public static void main(String[] args) throws IOException {
        String path = "src/test/resources";
        File file = new File(path);
        String resourcesPath = file.getAbsolutePath() + "/testEvent.json";

        String eventText = new String(Files.readAllBytes(Paths.get(resourcesPath)));

        long start = System.currentTimeMillis();
        for(int i=0;i<100000;i++){
            JSONObject eventJson = new JSONObject(eventText);
             new JSONColumnFormat(eventJson);
        }
        System.out.println("TotalMS: "+(System.currentTimeMillis()-start));
    }
}
