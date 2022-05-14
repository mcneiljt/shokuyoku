package com.mcneilio.shokuyoku;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Dispatcher {
    private static final Map<String, Class<?>> ENTRY_POINTS =
        new HashMap<String, Class<?>>();

    static {
        ENTRY_POINTS.put("main", App.class);
        ENTRY_POINTS.put("filter", Filter.class);
    }

    public static void main(final String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println(String.join("\n",
                "shokuyoku means 'appetite' in Japanese.",
                "This daemon devours data and digests it for storage.",
                "To begin listening for events pass the 'service' argument.",
                "To begin processing data pass the 'worker' argument."
            ));
            System.exit(1);
        }
        String command = "main";

        if (args.length > 1) {
            command = args[0];
        }
        final Class<?> entryPoint = ENTRY_POINTS.get(args[0]);
        if (entryPoint == null) {
            // throw exception, entry point doesn't exist
            System.out.println("Unknown entrypoint: "+args[0]);
            System.exit(1);
        }
        final String[] argsCopy =
            args.length > 1
                ? Arrays.copyOfRange(args, 1, args.length)
                : new String[0];
        entryPoint.getMethod("main", String[].class).invoke(null,
            (Object) argsCopy);
    }
}
