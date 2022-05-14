package com.mcneilio.shokuyoku.misc;

import com.mcneilio.shokuyoku.driver.BasicEventDriver;
import com.mcneilio.shokuyoku.driver.LocalStorageDriver;
import com.mcneilio.shokuyoku.util.MemoryDescriptionProvider;
import com.mcneilio.shokuyoku.util.TypeDescriptionProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.json.JSONObject;
import org.junit.Test;

import java.nio.file.Files;

public class OrcTest {
    @Test
    public void simpleWriteRead() throws Exception {
        String basePath = Files.createTempDirectory("tmpDirPrefix").toFile().getAbsolutePath();

        TypeDescription td = TypeDescription.createStruct();
        td = td.addField("id", TypeDescription.createString());
        td = td.addField("date", TypeDescription.createDate());

        MemoryDescriptionProvider memoryDescriptionProvider = new MemoryDescriptionProvider();

        memoryDescriptionProvider.setinstance("test_event", td);

        LocalStorageDriver localStorageDriver = new LocalStorageDriver(basePath);

        TypeDescription typeDescription = memoryDescriptionProvider.getInstance("test_database", "test_event");


        BasicEventDriver basicEventDriver = new BasicEventDriver( "test_event", "2022-01-01", typeDescription, localStorageDriver);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id", "123");
        JSONObject jsonObjectABC = new JSONObject();
        jsonObjectABC.put("id", "abc");
        basicEventDriver.addMessage(jsonObject);
        basicEventDriver.addMessage(jsonObjectABC);
        basicEventDriver.addMessage(new JSONObject());

        String filename = basicEventDriver.flush(false);
        Reader reader = OrcFile.createReader(new Path(filename), OrcFile.readerOptions(new Configuration()));
        RecordReader a = reader.rows();
        VectorizedRowBatch batch = reader.getSchema().createRowBatch(10);
        while (a.nextBatch(batch)) {
            LongColumnVector dateCol = (LongColumnVector) batch.cols[1];
            BytesColumnVector idCol = (BytesColumnVector) batch.cols[0];
            for (int rowNum = 0; rowNum < batch.size; rowNum++) {
                System.out.println("Row: "+dateCol.vector[rowNum]+" "+idCol.vector[rowNum].toString());
            }
        }
    }
}
