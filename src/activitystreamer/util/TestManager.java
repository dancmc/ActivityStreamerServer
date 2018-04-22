package activitystreamer.util;

import activitystreamer.server.Connection;
import activitystreamer.server.Control;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO remove this class
public class TestManager extends Thread{

    private static final Logger log = LogManager.getLogger();
    private boolean term;

    @Override
    public void run() {
        long min30 = 30*60*1000;

        while(!term) {
            try {
                Thread.sleep(20000);
                for(Connection connection :Control.getInstance().getConnections()){
                    if(System.currentTimeMillis() - connection.timeCreated >  min30){
                        connection.closeCon();
                    }
                }
            } catch (InterruptedException e) {
                log.error("Test manager interrupted");
            }
        }

    }

    public void setTerm(boolean term) {
        this.term = term;
        interrupt();
    }
}
