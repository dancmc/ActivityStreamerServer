package activitystreamer;


import activitystreamer.server.Control;
import activitystreamer.util.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Server {
    private static final Logger log = LogManager.getLogger();


    public static void main(String[] args) {

        log.info("starting server");

        // parse all the arguments given
        Settings.parseArguments(args);


        final Control c = Control.getInstance();
        c.initiateConnection();


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                // end the run loop in Control
                c.setTerm(true);
                c.interrupt();

                // clean up connections, mostly synchronously
                c.exit();
            }
        });
    }

}
