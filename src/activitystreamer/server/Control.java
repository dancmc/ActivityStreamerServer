package activitystreamer.server;

import activitystreamer.server.aux.Registration;
import activitystreamer.server.aux.ServerData;
import activitystreamer.util.JsonCreator;
import activitystreamer.util.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Single instance Control thread for the server
 */
public class Control extends Thread {
    private static final Logger log = LogManager.getLogger();
    private static Control control = null;

    private static Listener listener;
    private static CopyOnWriteArrayList<Connection> connections;
    private static AtomicInteger currentLoad = new AtomicInteger(0);
    private static boolean term = false;

    private static ConcurrentHashMap<String, String> userList = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, ServerData> serverList = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Registration> registrationPool = new ConcurrentHashMap<>();


    // only allow one instance of Control
    public static Control getInstance() {
        if (control == null) {
            control = new Control();
        }
        return control;
    }

    private Control() {

        // generate server id and output it
        Settings.setServerId(Settings.nextSecret());
        log.info("INFO - server id : " + Settings.getServerId());

        // initialize the connections array
        connections = new CopyOnWriteArrayList<>();

        // start a listener
        try {
            listener = new Listener();
        } catch (IOException e1) {
            log.fatal("FATAL - failed to startup a listening thread : " + e1);
            System.exit(-1);
        }

        start();
    }

    /**
     * Method to initiate outgoing connection if remote hostname supplied
     * From server skeleton code supplied
     */
    public void initiateConnection() {
        // make a connection to another server if remote hostname is supplied
        // also check that remote host and port are not local
        if (Settings.getRemoteHostname() != null) {

            if (Settings.getLocalHostname().equals(Settings.getRemoteHostname()) &&
                    Settings.getRemotePort() == Settings.getLocalPort()) {

                log.error("tried to connect to self on " + Settings.getRemoteHostname() + ":" + Settings.getRemotePort());
                System.exit(-1);
            }


            try {
                Connection outgoingConn = outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
                boolean writeResult = outgoingConn.writeMsg(JsonCreator.authenticate(Settings.getSecret()));

                if (!writeResult) {
                    throw new IOException("connection not open");
                }


            } catch (IOException e) {
                log.error("failed to make connection to " + Settings.getRemoteHostname() + ":" + Settings.getRemotePort() + " : " + e);
                System.exit(-1);
            }
        }
    }

    // Main thread loop
    @Override
    public void run() {
        log.info("INFO - starting control loop with activity interval of " + Settings.getActivityInterval() + " milliseconds");
        while (!term) {
            // do something with x second intervals in between
            try {
                Thread.sleep(Settings.getActivityInterval());
            } catch (InterruptedException e) {
                log.error("ERROR - received interrupt, system is shutting down");
                break;
            }

            if (!term) {
                // currently pretty pointless since always returns false
                // in case external thread sets term to true during execution of if block
                term = term || doActivity();
            }
        }
    }

    /**
     * Called by shutdown hook to cleanup connections before terminating
     */
    public void exit() {
        log.info("INFO - cleaning up " + connections.size() + " connections for shutdown");

        /*
         * clean up connections ?mostly synchronously
         * not entirely sure if there's a reliable way to asynchronously terminate Connection loops
         * while waiting for all of them
         */
        for (Connection connection : connections) {
            connection.closeCon();
        }
        listener.setTerm(true);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {

        }
    }

    //

    /**
     * Generate and broadcast a server announce here
     *
     * @return true if control should terminate
     */
    private boolean doActivity() {

        // calculate client load
        // can only count clients who have logged in at least once in the past
        int load = 0;
        for (Connection connection : connections) {
            if (connection.isClient()) {
                load++;
            }
        }

        String serverAnnounce = JsonCreator.serverAnnounce(
                Settings.getServerId(),
                load,
                Settings.getLocalHostname(),
                Settings.getLocalPort());


        for (Connection connection : connections) {
            if (connection.isServer() && connection.isLoggedIn()) {
                connection.writeMsg(serverAnnounce);
            }
        }
        log.debug("DEBUG - generated and broadcast SERVER_ANNOUNCE " + serverAnnounce);


        // *BUG* should probably close client connections after removing rego attempt, but not in spec
        // safe to remove from ConcurrentHashMap while iterating
//        for(Map.Entry<String, Registration> entry : registrationPool.entrySet()){
//            if(System.currentTimeMillis() - entry.getValue().getStartTime()>10000){
//
//                registrationPool.remove(entry.getKey());
//                log.debug("timeout on registration for "+entry.getKey() + ", removed");
//            }
//        }

        return false;
    }


    /**
     * Method for processing any messages that require action involving connections other than originating one
     * Synchronised method ensures only one message processed globally at a time
     * As much processing as possible takes place on individual connection threads to reduce blocking
     * <p>
     * Shouldn't be deadlock with writing out to other connections from here
     *
     * @param processCon connection receiving message
     * @param json       JSONObject from message string if parsable
     * @return true if connection should terminate based on message
     */
    public synchronized boolean process(Connection processCon, JSONObject json) {

        try {
            String command = json.getString("command");

            switch (command) {
                case "ACTIVITY_MESSAGE": {

                    // forward to all other servers/clients (connection has already validated info)
                    // extract activity object and add user field
                    JSONObject processedActivityObject = json
                            .getJSONObject("activity")
                            .put("authenticated_user", processCon.getClientId());

                    // put processed activity into a broadcast message and send to other servers/clients
                    String activityBroadcast = JsonCreator.activityBroadcast(processedActivityObject);
                    Pair<Integer, Integer> result = broadcastToAll(processCon, activityBroadcast, true);
                    log.info("ACTIVITY_MESSAGE - forwarded to " + result.fst + " servers, " + result.snd + " clients");

                    break;
                }

                case "SERVER_ANNOUNCE": {

                    // this assumes no servers quit or crash so there is no need to ever prune the server list

                    String id = json.getString("id");
                    int load = json.getInt("load");
                    String hostname = json.getString("hostname");
                    int port = json.getInt("port");

                    // update server list
                    ServerData server = serverList.get(id);
                    if (server == null) {
                        serverList.put(id, new ServerData(id, load, hostname, port));
                    } else {
                        server.setLoad(load);
                    }

                    // forward to all other servers
                    broadcastToServers(processCon, json.toString(), false);
                    log.info("SERVER_ANNOUNCE - from " + id + " at " + hostname + ":" + port + ", load : " + load);

                    break;

                }

                case "ACTIVITY_BROADCAST": {

                    Pair<Integer, Integer> result = broadcastToAll(processCon, json.toString(), false);
                    log.info("ACTIVITY_BROADCAST received - forwarded to " + result.fst + " servers, " + result.snd + " clients");

                    break;
                }

                case "REGISTER": {

                    String username = json.getString("username");
                    String secret = json.getString("secret");

                    log.info("REGISTER - attempting registration for " + username + " with secret " + secret);


                    // check that username isn't null
                    if (username == null) {
                        String error = "null username";
                        return processCon.termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - " + error);
                    }

                    // check that the username isn't already in storage
                    if (userExists(username)) {
                        String error = "username " + username + " already registered";
                        return processCon.termConnection(JsonCreator.registerFailed(error), "REGISTER_FAILED - " + error);
                    }

                    // check that secret isn't null
                    if (secret == null) {
                        String error = "null secret";
                        return processCon.termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - " + error);
                    }

                    // check that connection doesn't belong to a server
                    if (processCon.isServer()) {
                        String error = "not a client";
                        return processCon.termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - " + error);
                    }

                    // check that not already logged in
                    if (processCon.isLoggedIn()) {
                        String error = "register message received from client already logged in";
                        return processCon.termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - " + error);
                    }

                    // count number of current other servers (since lock_allowed has no server_id)
                    // and no servers quit or crash
                    int currentServerCount = serverList.size();

                    if (currentServerCount == 0) {
                        registerSuccessfulUser(processCon, username, secret);
                        return false;
                    }

                    // add username to registration pool
                    addToRegistrationPool(processCon, username, secret, currentServerCount);

                    // also add username/secret to local storage first (as per Aaron's test server behaviour)
                    addUser(username, secret);

                    // send out lock request
                    String lockRequest = JsonCreator.lockRequest(username, secret);
                    broadcastToServers(processCon, lockRequest, false);
                    log.info("REGISTER - lock request broadcast");

                    // replies from other servers will be processed when lock_allowed and lock_denied msgs arrive

                    break;
                }

                case "LOCK_REQUEST": {

                    // validate first
                    String username = json.getString("username");
                    String secret = json.getString("secret");

                    // check that sender server is authenticated
                    if (!processCon.isLoggedIn() || !processCon.isServer()) {
                        String error = "server not authenticated";
                        return processCon.termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - " + error);
                    }

                    // forward the lock request
                    int result = broadcastToServers(processCon, json.toString(), false);
                    log.info("LOCK_REQUEST - forwarded to " + result + " servers");

                    // check if username is known and generate broadcast for denied or allowed
                    // theoretically if denied then don't even need to bother to forward lock request
                    String storedSecret = getSecretForUser(username);
                    if (!userExists(username)) {
                        String lockAllowed = JsonCreator.lockAllowed(username, secret);
                        for (Connection connection : connections) {
                            if (connection.isServer() && connection.isLoggedIn()) {
                                connection.writeMsg(lockAllowed);
                                addUser(username, secret);
                            }
                        }
                        log.info("LOCK_REQUEST - broadcast LOCK_ALLOWED in response");
                    } else {
                        // send LOCK_DENIED if username known regardless of secret (as per discussion board)
                        String lockDenied = JsonCreator.lockDenied(username, secret);
                        for (Connection connection : connections) {
                            if (connection.isServer() && connection.isLoggedIn()) {
                                connection.writeMsg(lockDenied);
                            }
                        }
                        log.info("LOCK_REQUEST - broadcast LOCK_DENIED in response");
                    }

                    break;
                }

                case "LOCK_DENIED": {

                    // validate
                    String username = json.getString("username");
                    String secret = json.getString("secret");

                    // check that sending server authenticated
                    if (!processCon.isLoggedIn() || !processCon.isServer()) {
                        String error = "server not authenticated";
                        return processCon.termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - " + error);
                    }

                    // remove from username from local storage if secret matches
                    String storedSecret = getSecretForUser(username);
                    if (storedSecret != null && storedSecret.equals(secret)) {
                        removeUser(username);
                    }

                    // forward to other servers
                    int result = broadcastToServers(processCon, json.toString(), false);
                    log.info("LOCK_DENIED - forwarded to " + result + " servers");

                    // if is the server originating the request, send denied, close connection, and remove pending rego
                    Registration rego = getRegistrationFromPool(username);
                    if (rego != null) {
                        removeRegistrationFromPool(username);
                        String error = username + " already registered in the system";
                        return processCon.termConnection(JsonCreator.registerFailed(error), "REGISTER_FAILED : " + error);
                    }

                    break;

                }

                case "LOCK_ALLOWED": {

                    // validate
                    String username = json.getString("username");
                    String secret = json.getString("secret");


                    // check that sending server authenticated
                    if (!processCon.isLoggedIn() || !processCon.isServer()) {
                        String error = "server not authenticated";
                        return processCon.termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - " + error);
                    }

                    // forward to other servers
                    int result = broadcastToServers(processCon, json.toString(), false);
                    log.info("LOCK_ALLOWED - forwarded to " + result + " servers");

                    // if is the server originating the request, decrement the count
                    Registration rego = getRegistrationFromPool(username);
                    if (rego != null) {
                        if (rego.getUsername().equals(username) && rego.getSecret().equals(secret)) {
                            int latestCount = rego.decrementAndGetAllowsNeeded();
                            log.info("REGISTER status for " + username + " : waiting for " + latestCount + " more LOCK_ALLOWED");

                            // if not waiting for anymore results, add user to list
                            if (latestCount == 0) {
                                registerSuccessfulUser(rego.getConnection(), rego.getUsername(), rego.getSecret());
                                return false;
                            }
                        }
                    }

                    break;
                }

                default: {
                    String error = "unknown command";
                    return processCon.termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - " + error);
                }
            }

        } catch (JSONException e) {

            // catches all malformed messages
            String error = "JSON parse exception : " + e.getMessage();
            return processCon.termConnection(JsonCreator.invalidMessage(error), "ERROR - " + error);
        }

        return false;
    }


    // CONNECTION RELATED UTILITY METHODS

    // Cleanup after a connection has been closed
    public void connectionClosed(Connection con) {
        connections.remove(con);
    }

    /**
     * A new incoming connection has been established, added to connection list, and reference to it is returned
     *
     * @param s socket object of established connection
     * @return connection object created from socket object
     * @throws IOException if anything goes wrong accessing data streams from socket
     */
    public Connection incomingConnection(Socket s) throws IOException {
        log.debug("DEBUG - incoming connection : " + Settings.socketAddress(s));
        Connection c = new Connection(s, false);
        connections.add(c);
        return c;
    }

    /**
     * A new outgoing connection has been established, added to connection list, and reference to it is returned
     *
     * @param s socket object of established connection
     * @return connection object created from socket object
     * @throws IOException if anything goes wrong accessing data streams from socket
     */
    public Connection outgoingConnection(Socket s) throws IOException {
        log.debug("DEBUG - outgoing connection: " + Settings.socketAddress(s));
        Connection c = new Connection(s, true);
        connections.add(c);
        return c;

    }


    // OTHER UTILITY METHODS

    /**
     * Simple way to broadcast to all logged in client/server connections
     *
     * @param processCon connection which received message triggering broadcast
     * @param broadcast  string to be broadcast
     * @return pair of counts of servers & clients successfully sent to
     */
    private Pair<Integer, Integer> broadcastToAll(Connection processCon, String broadcast, boolean includeSender) {

        int serverCount = 0;
        int clientCount = 0;

        // forward to all other authenticated connections (connection has already validated info)
        for (Connection connection : connections) {
            if ((includeSender || connection != processCon) && connection.isLoggedIn()) {
                if (connection.writeMsg(broadcast)) {
                    if (connection.isClient()) {
                        clientCount++;
                    } else if (connection.isServer()) {
                        serverCount++;
                    }
                }
            }
        }
        return new Pair<>(serverCount, clientCount);
    }

    /**
     * Simple way to broadcast to all logged in server connections
     *
     * @param processCon connection which received message triggering broadcast
     * @param broadcast  string to be broadcast
     * @return counts of servers successfully sent to
     */
    private int broadcastToServers(Connection processCon, String broadcast, boolean includeSender) {

        int count = 0;

        // forward to all other servers (connection has already validated info)
        for (Connection connection : connections) {
            if (connection.isServer() && (includeSender || connection != processCon) && connection.isLoggedIn()) {
                if (connection.writeMsg(broadcast)) {
                    count++;
                }
            }
        }
        return count;
    }


    // REGISTRATION RELATED UTILITY METHODS

    /**
     * Add user to userlist
     *
     * @param username username string
     * @param secret   secret string
     */
    private void addUser(String username, String secret) {
        userList.put(username, secret);
    }

    /**
     * Add user to list if server sent original lock_request and received correct allows
     *
     * @param connection connection originating lock_request
     * @param username   username string
     * @param secret     secret string
     */
    private void registerSuccessfulUser(Connection connection, String username, String secret) {
        addUser(username, secret);
        if (connection != null) {
            String message = "register success for " + username;
            connection.writeMsg(JsonCreator.registerSuccess(message));
            log.info("REGISTER_SUCCESS - " + message);
        }
    }

    /**
     * Remove user from list
     *
     * @param user username string
     */
    private void removeUser(String user) {
        if (user != null) {
            userList.remove(user);
        }
    }

    public static boolean userExists(String user) {
        return user != null && userList.containsKey(user);
    }

    public static String getSecretForUser(String user) {
        if (user == null) {
            return null;
        } else {
            return userList.get(user);
        }
    }

    public static void addToRegistrationPool(Connection con, String username, String secret, int currentServerCount) {
        registrationPool.put(username, new Registration(con, username, secret, currentServerCount));
    }

    public static boolean checkRegistrationPoolForUser(String username) {
        return username != null && registrationPool.containsKey(username);
    }

    public static int getRegistrationRemainingAllowedCount(String username) {
        if (username == null) {
            return -1;
        } else {
            Registration rego = registrationPool.get(username);
            if (rego == null) {
                return -1;
            } else {
                return rego.getAllowsNeeded();
            }
        }
    }

    public static void removeRegistrationFromPool(String username) {
        if (username != null) {
            registrationPool.remove(username);
        }
    }

    public static Registration getRegistrationFromPool(String username) {
        if (username == null) {
            return null;
        } else {
            return registrationPool.get(username);
        }
    }

    // MISCELLANEOUS GETTERS AND SETTERS

    public final void setTerm(boolean t) {
        term = t;
    }

    public static int getCurrentLoad() {
        return currentLoad.get();
    }

    public static void incrementCurrentLoad() {
        currentLoad.incrementAndGet();
    }

    public static void decrementCurrentLoad() {
        currentLoad.decrementAndGet();
    }

    public final CopyOnWriteArrayList<Connection> getConnections() {
        return connections;
    }

    public static ConcurrentHashMap<String, ServerData> getServerList() {
        return serverList;
    }

    private class Pair<A,B>{
        public A fst;
        public B snd;

        Pair(A first, B second){
            fst = first;
            snd = second;
        }
    }
}
