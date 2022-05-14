package com.mcneilio.shokuyoku.util;

import com.mcneilio.shokuyoku.format.JSONColumnFormat;
import com.mcneilio.shokuyoku.util.StringNormalizer;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

public class NormalizeKeyTest {

    @Test
    public void simpleLowerCase() throws Exception {
        assertTrue(StringNormalizer.normalizeKey("asd").equals("asd"));
    }

    @Test
    public void simpleSingleUpperCase() throws Exception {
        assertTrue(StringNormalizer.normalizeKey("catDog").equals("cat_dog"));
    }

//    @Test
//    public void simpleDoubleUpperCase() throws Exception {
//        assertTrue(StringNormalizer.normalizeKey("catDOg").equals("cat_dog"));
//    }

    @Test
    public void simpleLeadingUpperCase() throws Exception {
        assertTrue(StringNormalizer.normalizeKey("Dog").equals("dog"));
    }

    @Test
    public void simpleNumberCase() throws Exception {
        assertTrue(StringNormalizer.normalizeKey("c3Dog").equals("c3_dog"));
    }

    @Test
    public void simpleDotCase() throws Exception {
        assertTrue(StringNormalizer.normalizeKey("cat.dog").equals("cat_dog"));
    }
    @Test
    public void simpleDashCase() throws Exception {
        assertTrue(StringNormalizer.normalizeKey("cat-dog").equals("cat_dog"));
    }

    @Test
    public void simpleDotCaseTopic() throws Exception {
        assertTrue(StringNormalizer.normalizeTopic("cat.dog").equals("cat.dog"));
    }

    @Test
    public void leadingUppereCaseTopic() throws Exception {
        assertTrue(StringNormalizer.normalizeTopic("MRCat").equals("mr_cat"));
    }
}
