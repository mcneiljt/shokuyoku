package com.mcneilio;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class NormalizeKeyTest {

    @Test
    public void simpleLowerCase() throws Exception {
        assertTrue(JSONColumnFormat.normalizeKey("asd").equals("asd"));
    }

    @Test
    public void simpleSingleUpperCase() throws Exception {
        assertTrue(JSONColumnFormat.normalizeKey("catDog").equals("cat_dog"));
    }

    @Test
    public void simpleDoubleUpperCase() throws Exception {
        assertTrue(JSONColumnFormat.normalizeKey("catDOg").equals("cat_dog"));
    }

    @Test
    public void simpleLeadingUpperCase() throws Exception {
        assertTrue(JSONColumnFormat.normalizeKey("Dog").equals("dog"));
    }

    @Test
    public void simpleNumberCase() throws Exception {
        assertTrue(JSONColumnFormat.normalizeKey("c3Dog").equals("c3_dog"));
    }

    @Test
    public void simpleDogCase() throws Exception {
        assertTrue(JSONColumnFormat.normalizeKey("cat.dog").equals("cat_dog"));
    }
}
