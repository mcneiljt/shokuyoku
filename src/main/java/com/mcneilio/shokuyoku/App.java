package com.mcneilio.shokuyoku;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;

/**
 * Entrypoint to the shokuyoku daemon
 */
public class App {
    public static void main(String[] args) throws URISyntaxException, FileNotFoundException {
        String newLine = System.getProperty("line.separator");

        if (args.length < 1) {
            System.out.println(String.join(newLine,
                "shokuyoku means 'appetite' in Japanese.",
                "This daemon devours data and digests it for storage.",
                "To begin listening for events pass the 'service' argument.",
                "To begin processing data pass the 'worker' argument."
            ));
            System.exit(1);
        }

        if (args[0].equals("service") || args[0].equals("standalone")) {
            Service service = new Service();
            service.start();
        }

        if (args[0].equals("worker") || args[0].equals("standalone")) {
            IWorker worker;
            if(System.getenv("WORKER_VERSION") != null && System.getenv("WORKER_VERSION").equalsIgnoreCase("json")) {
                worker = new WorkerJSON();
            } else {
                worker = new Worker();
            }

            // TODO replace this with something better
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        worker.start();
                    } catch (Exception e) {
                        // TODO In standalone mode, safely stop other running workers (ie filter)
                        throw new RuntimeException(e);
                    }

                }
            }).start();
        }

        if (args[0].equals("filter") || args[0].equals("standalone")) {
            Filter filter = new Filter();

            // TODO replace this with something better
            new Thread(new Runnable() {
                @Override
                public void run() {
                    filter.start();

                }
            }).start();
        }

        if (args[0].equals("error_worker") || args[0].equals("standalone")) {
            ErrorWorker errorWorker = new ErrorWorker();

            // TODO replace this with something better
            new Thread(new Runnable() {
                @Override
                public void run() {
                    errorWorker.start();
                }
            }).start();
        }
    }
}
