package activitystreamer.server;

import activitystreamer.util.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Listener thread to accept new incoming connections and pass them into threads of their own
 */
public class Listener extends Thread {
    private static final Logger log = LogManager.getLogger();
    private ServerSocket serverSocket;
    private boolean term = false;
    private int portnum;

    public Listener() throws IOException {
        portnum = Settings.getLocalPort(); // keep our own copy in case it changes later
        serverSocket = new ServerSocket(portnum);
        start();
    }

    @Override
    public void run() {
        log.info("INFO - listening for new connections on " + portnum);
        while (!term) {
            Socket clientSocket;
            try {
                // client socket could be a server or client connecting, but unknown which at this point
                clientSocket = serverSocket.accept();
                Control.getInstance().incomingConnection(clientSocket);

            } catch (IOException e) {
                log.error("ERROR - listener server socket received exception, shutting down");
                term = true;
            }
        }
    }

    /**
     * To terminate listener thread when closing down
     * @param term whether listener should terminate
     */
    public void setTerm(boolean term) {
        this.term = term;
        try {
            log.info("INFO - closing server socket");
            serverSocket.close();
        } catch (IOException e){
            log.error("ERROR - error closing server socket");
        }
    }


}
