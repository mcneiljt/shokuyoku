package com.mcneilio.shokuyoku;

/**
 * Entrypoint to the shokuyoku daemon
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        String newLine = System.getProperty("line.separator");

        if (args.length < 1)
        {
            System.out.println(String.join(newLine,
                    "shokuyoku means 'appetite' in Japanese.",
                    "This daemon devours data and digests it for storage.",
                    "To begin listening for events pass the 'service' argument.",
                    "To begin processing data pass the 'worker' argument."
            ));
        }
        else if (args[0].equals("service"))
        {
            Service service = new Service();
            service.start();
        }
        else if (args[0].equals("worker"))
        {
            Worker worker = new Worker();
            worker.start();
        }
    }
}
