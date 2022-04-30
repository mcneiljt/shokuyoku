package com.mcneilio.shokuyoku.util;

import java.util.regex.Pattern;

public class StringNormalizer {

    public static String normalizeKey(String key) {
        return normalizeTopic(key).replaceAll("\\.","_");
    }

    private static Pattern pattern1 = Pattern.compile("[^a-zA-Z\\d.]");
    private static Pattern pattern2 = Pattern.compile("([A-Z]+)([A-Z][a-z])");
    private static Pattern pattern3 = Pattern.compile("([a-z\\d])([A-Z])");

    public static String normalizeTopic(String topicName) {
        String step1 = pattern1.matcher(topicName).replaceAll("_");
        String step2 = pattern2.matcher(step1).replaceAll("$1_$2");
        return pattern3.matcher(step2).replaceAll("$1_$2").toLowerCase();
//        return topicName.replaceAll(pattern1, "_")
//            .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2") // Not sure which case this was solving for
//            .replaceAll("([a-z\\d])([A-Z])", "$1_$2")
//            .toLowerCase();
    }
}
