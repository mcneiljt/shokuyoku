package com.mcneilio.shokuyoku.driver;

import com.mcneilio.shokuyoku.helpers.SimpleTypeDescriptionProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for BasicEventDriver
 */
public class BasicEventDriverTest {
    @Test
    public void firehoseCanSerializeAndDeserializeLittleEndian() throws IOException {
        String basePath = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();
        LocalStorageDriver localStorageDriver = new LocalStorageDriver(basePath);

        SimpleTypeDescriptionProvider simpleTypeDescriptionProvider = new SimpleTypeDescriptionProvider();
        Map<String, String> columns = new HashMap<>();
        columns.put("count", "bigint");
        columns.put("date", "date");
        simpleTypeDescriptionProvider.addTypeDescription("event_name", columns);

        BasicEventDriver basicEventDriver = new BasicEventDriver("event_name", "2022-01-01", simpleTypeDescriptionProvider.getInstance("test", "event_name"), localStorageDriver);

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("count", 1);
        basicEventDriver.addMessage(jsonObject);

        basicEventDriver.flush(true);

        System.out.println("ASD");

        String[] pathnames = new File(basePath).list();


        Reader reader = OrcFile.createReader(new Path(basePath + "/" + pathnames[0]), OrcFile.readerOptions(new Configuration()));

        int rowCount = 0;

        //reader.

        RecordReader records = reader.rows(reader.options());
        VectorizedRowBatch batch = reader.getSchema().createRowBatch(1000);

        assertThat(batch.cols.length).isEqualTo(2);

        while (records.nextBatch(batch)) {
            for (int rowNum = 0; rowNum < batch.size; rowNum++) {
                rowCount++;
                // Read rows from the batch
//                Map<String, Object> map = new HashMap<>();
//                map.put("order_id", orderIdColumnVector.vector[rowNum]);
//                map.put("item_name", itemNameColumnVector.toString(rowNum));
//                map.put("price", priceColumnVector.vector[rowNum]);
//                rows.add(map);
            }
        }
        System.out.println("ASD");
        assertThat(rowCount).isEqualTo(1);
    }
}
