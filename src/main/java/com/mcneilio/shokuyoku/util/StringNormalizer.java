package com.mcneilio.shokuyoku.util;

import java.util.regex.Pattern;

public class StringNormalizer {

    private static final Pattern pattern0 = Pattern.compile("\\.");


    public static String normalizeKey(String key) {
        return pattern0.matcher(normalizeTopic(key)).replaceAll("_");
    }

    private static final Pattern pattern1 = Pattern.compile("[^a-zA-Z\\d.]");
    private static final Pattern pattern2 = Pattern.compile("([A-Z]+)([A-Z][a-z])");
    private static final Pattern pattern3 = Pattern.compile("([a-z\\d])([A-Z])");

    public static String normalizeTopic(String topicName) {
        String step1 = pattern1.matcher(topicName).replaceAll("_");
        String step2 = pattern2.matcher(step1).replaceAll("$1_$2");
        return pattern3.matcher(step2).replaceAll("$1_$2").toLowerCase();
    }
}
