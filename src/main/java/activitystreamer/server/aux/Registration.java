package activitystreamer.server.aux;


import activitystreamer.server.Connection;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Project : Activity Streamer Server
 * Author : Daniel Chan (mchan@student.unimelb.edu.au)
 * Date : 22 Mar 2018
 */

/**
 * Data class to hold pending registration attempts
 */
public class Registration {

    private Connection connection;
    private String username;
    private String secret;
    private AtomicInteger allowsNeeded;
    private long startTime = System.currentTimeMillis();

    public Registration(Connection connection, String username, String secret, int allowsNeeded){
        this.connection = connection;
        this.username = username;
        this.secret = secret;
        this.allowsNeeded = new AtomicInteger(allowsNeeded);
    }

    public Connection getConnection() {
        return connection;
    }

    public String getUsername() {
        return username;
    }

    public String getSecret() {
        return secret;
    }

    public int decrementAndGetAllowsNeeded(){
        return allowsNeeded.decrementAndGet();
    }

    public int incrementAndGetAllowsNeeded(){
        return allowsNeeded.incrementAndGet();
    }

    public int getAllowsNeeded(){
        return allowsNeeded.get();
    }


    public long getStartTime() {
        return startTime;
    }
}
