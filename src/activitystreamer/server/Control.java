package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import activitystreamer.util.JsonCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.JSONException;
import org.json.JSONObject;
import activitystreamer.server.Connection.ConnectionType;

public class Control extends Thread {
    private static final Logger log = LogManager.getLogger();
    private static Control control = null;
    private static AtomicInteger currentLoad = new AtomicInteger(0);

    private static CopyOnWriteArrayList<Connection> connections;
    private static boolean term = false;
    private static Listener listener;

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
        log.info("server id : " + Settings.getServerId());

        // initialize the connections array
        connections = new CopyOnWriteArrayList<>();

        // start a listener
        try {
            listener = new Listener();
        } catch (IOException e1) {
            log.fatal("failed to startup a listening thread: " + e1);
            System.exit(-1);
        }

        start();
    }

    public void initiateConnection() {
        // make a connection to another server if remote hostname is supplied
        // also check that remote host and port are not local
        if (Settings.getRemoteHostname() != null) {

            if (!Settings.getLocalHostname().equals(Settings.getRemoteHostname()) &&
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


    // TODO i *think* there shouldn't be deadlock with writing out to other connections from here
    /*
     * Processing incoming messages from the connection.
     * Return true if the connection should close.
     */
    public synchronized boolean process(Connection con, JSONObject json) {

        try {
            String command = json.getString("command");

            switch (command) {
                case "ACTIVITY_MESSAGE": {

                    JSONObject processedActivityObject = json
                            .getJSONObject("activity")
                            .put("authenticated_user", con.getClientId());

                    String activityBroadcast = JsonCreator.activityBroadcast(processedActivityObject);
                    for (Connection connection : connections) {
                        if (connection != con && connection.isLoggedIn()) {
                            connection.writeMsg(activityBroadcast);
                        }
                    }


                    break;
                }

                case "SERVER_ANNOUNCE": {

                    // assuming no servers quit or crash
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
                    String serverAnnounce = json.toString();
                    for (Connection connection : connections) {
                        if (connection != con && connection.isServer() && connection.isLoggedIn()) {
                            connection.writeMsg(serverAnnounce);
                        }
                    }
                    break;

                }

                case "ACTIVITY_BROADCAST": {

                    // forward to all other servers (connection has already validated)
                    String activityBroadcast = json.toString();
                    for (Connection connection : connections) {
                        if (connection != con && connection.isLoggedIn()) {
                            connection.writeMsg(activityBroadcast);
                        }
                    }
                    break;
                }

                case "REGISTER": {

                    String username = json.getString("username");
                    String secret = json.getString("secret");

                    // check that the username isn't already in storage
                    if (userExists(username)) {
                        String error = "Error : User already registered";
                        return con.termConnection(JsonCreator.registerFailed(error), error);
                    }

                    // check that connection doesn't belong to a server
                    if (con.isServer()) {
                        String error = "Error : Not a client";
                        return con.termConnection(JsonCreator.invalidMessage(error), error);
                    }

                    // check that not already logged in
                    if (con.isLoggedIn()) {
                        String error = "Error : Register message received from client already logged in";
                        return con.termConnection(JsonCreator.invalidMessage(error), error);
                    }

                    // count number of current other servers (since lock_allowed has no server_id)
                    // and no servers quit or crash
                    int currentServerCount = serverList.size();

                    if (currentServerCount == 0) {
                        addUser(con, username, secret);
                        return false;
                    }

                    // add username to registration pool
                    addToRegistrationPool(con, username, secret, currentServerCount);

                    // send out lock request
                    String lockRequest = JsonCreator.lockRequest(username, secret);
                    for (Connection connection : connections) {
                        if (connection != con && connection.isServer() && connection.isLoggedIn()) {
                            connection.writeMsg(lockRequest);
                        }
                    }

                    // replies from other servers will be processed through lock_allowed and lock_denied msgs
                    // start a timeout, close connection and remove from registration pool if neither allowed nor denied after x ms
                    // todo

                    break;
                }

                case "LOCK_REQUEST": {

                    // validate first
                    String username = json.getString("username");
                    String secret = json.getString("secret");

                    // check that sender server is authenticated
                    if (!con.isLoggedIn() || !con.isServer()) {
                        String error = "Error : Server not authenticated";
                        return con.termConnection(JsonCreator.invalidMessage(error), error);
                    }

                    // forward the lock request
                    String lockRequest = json.toString();
                    for (Connection connection : connections) {
                        if (connection != con && connection.isServer() && connection.isLoggedIn()) {
                            connection.writeMsg(lockRequest);
                        }
                    }

                    // check if username is known and generate broadcast for denied or allowed
                    // theoretically if denied then don't even need to bother to forward lock request
                    String storedSecret = getSecretForUser(username);
                    if (!userExists(username)) {
                        String lockAllowed = JsonCreator.lockAllowed(username, secret);
                        for (Connection connection : connections) {
                            if (connection.isServer() && connection.isLoggedIn()) {
                                connection.writeMsg(lockAllowed);
                                userList.put(username.toLowerCase(), secret);
                            }
                        }
                    } else if (storedSecret != null && !storedSecret.equals(secret)) {
                        String lockDenied = JsonCreator.lockDenied(username, secret);
                        for (Connection connection : connections) {
                            if (connection.isServer() && connection.isLoggedIn()) {
                                connection.writeMsg(lockDenied);
                            }
                        }
                    }

                    break;
                }

                case "LOCK_DENIED": {

                    // validate
                    String username = json.getString("username");
                    String secret = json.getString("secret");

                    // check that sending server authenticated
                    if (!con.isLoggedIn() || !con.isServer()) {
                        String error = "Error : Server not authenticated";
                        return con.termConnection(JsonCreator.invalidMessage(error), error);
                    }

                    // remove from username from local storage if secret matches
                    String storedSecret = getSecretForUser(username);
                    if (storedSecret != null && storedSecret.equals(secret)) {
                        removeUser(username);
                    }

                    // forward to other servers
                    String lockDenied = json.toString();
                    for (Connection connection : connections) {
                        if (connection != con && connection.isServer() && connection.isLoggedIn()) {
                            connection.writeMsg(lockDenied);
                        }
                    }

                    // if is the server originating the request, close connection and remove pending rego
                    Registration rego = getRegistrationFromPool(username);
                    if (rego != null) {
                        removeRegistrationFromPool(username);
                        String registerFailed = JsonCreator.registerFailed(username + " already registered in the system");
                        return con.termConnection(registerFailed, registerFailed);
                    }

                    break;

                }

                case "LOCK_ALLOWED": {

                    // validate
                    String username = json.getString("username");
                    String secret = json.getString("secret");

                    // check that sending server authenticated
                    if (!con.isLoggedIn() || !con.isServer()) {
                        String invalidMessage = JsonCreator.invalidMessage("Error : Server not authenticated");
                        return con.termConnection(JsonCreator.invalidMessage(invalidMessage), invalidMessage);
                    }

                    // forward to other servers
                    String lockAllowed = json.toString();
                    for (Connection connection : connections) {
                        if (connection != con && connection.isServer() && connection.isLoggedIn()) {
                            connection.writeMsg(lockAllowed);
                        }
                    }

                    // if is the server originating the request, decrement the count
                    Registration rego = getRegistrationFromPool(username);
                    if (rego != null) {
                        if(rego.getUsername().equalsIgnoreCase(username)&&rego.getSecret().equalsIgnoreCase(secret)) {
                            int latestCount = rego.decrementAndGetAllowsNeeded();
                            if (latestCount == 0) {
                                addUser(rego.getConnection(), rego.getUsername(), rego.getSecret());
                                return false;
                            }
                        }
                    }

                    break;
                }

                default: {
                    String invalidMessage = JsonCreator.invalidMessage("Error : Unknown command");
                    return con.termConnection(invalidMessage, invalidMessage);
                }
            }

        } catch (JSONException e) {
            String error = "JSON parse exception : " + e.getMessage();
            return con.termConnection(JsonCreator.invalidMessage(error), error);
        }


        return false;
    }

    /*
     * The connection has been closed by the other party.
     */
    public synchronized void connectionClosed(Connection con) {
        connections.remove(con);
    }

    // TODO only counting client connections into load
    /*
     * A new incoming connection has been established, and a reference is returned to it
     */
    public synchronized Connection incomingConnection(Socket s) throws IOException {
        log.debug("incomming connection: " + Settings.socketAddress(s));
        Connection c = new Connection(s, false);
        connections.add(c);
        return c;

    }

    /*
     * A new outgoing connection has been established, and a reference is returned to it
     */
    public synchronized Connection outgoingConnection(Socket s) throws IOException {
        log.debug("outgoing connection: " + Settings.socketAddress(s));
        Connection c = new Connection(s, true);
        connections.add(c);
        return c;

    }

    @Override
    public void run() {
        log.info("using activity interval of " + Settings.getActivityInterval() + " milliseconds");
        while (!term) {
            // do something with 5 second intervals in between
            try {
                Thread.sleep(Settings.getActivityInterval());
            } catch (InterruptedException e) {
                log.info("received an interrupt, system is shutting down");
                break;
            }
            if (!term) {
                term = doActivity();
            }

        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e){

        }

        log.info("closing " + connections.size() + " connections");
        // clean up
        for (Connection connection : connections) {
            connection.closeCon();
        }
        listener.setTerm(true);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e){

        }
        System.exit(-1);


    }

    // generate a server announce here
    // also clean up all registrations that have been pending for 10s
    private boolean doActivity() {

        // TODO check whether really only client load needed
        // calculate client load
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
        log.debug("doing activity : broadcast "+serverAnnounce);


        // TODO should probably close client connections after removing rego attempt, but not in spec
        // should be safe to remove from ConcurrentHashMap while iterating
        for(Map.Entry<String, Registration> entry : registrationPool.entrySet()){
            if(System.currentTimeMillis() - entry.getValue().getStartTime()>10000){

                // TODO should I send register failed?

                registrationPool.remove(entry.getKey());
                log.debug("timeout on registration for "+entry.getKey() + ", removed");
            }
        }

        return false;
    }

    public final void setTerm(boolean t) {
        term = t;
        if (term) interrupt();
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

    public static void addUser(Connection connection, String username, String secret) {
        userList.put(username.toLowerCase(), secret);
        if(connection!=null) {
            String registerSuccess = JsonCreator.registerSuccess("register success for " + username);
            connection.writeMsg(registerSuccess);
        }
    }

    public static void removeUser(String user) {
        if (user != null) {
            userList.remove(user.toLowerCase());
        }
    }

    public static boolean userExists(String user) {
        return user != null && userList.containsKey(user.toLowerCase());
    }

    public static String getSecretForUser(String user) {
        if (user == null) {
            return null;
        } else {
            return userList.get(user.toLowerCase());
        }
    }

    public static void addToRegistrationPool(Connection con, String username, String secret, int currentServerCount) {
        registrationPool.put(username, new Registration(con, username.toLowerCase(), secret, currentServerCount));
    }

    public static boolean checkRegistrationPoolForUser(String username) {
        return username != null && registrationPool.containsKey(username.toLowerCase());
    }

    public static int getRegistrationRemainingAllowedCount(String username) {
        if (username == null) {
            return -1;
        } else {
            Registration rego = registrationPool.get(username.toLowerCase());
            if (rego == null) {
                return -1;
            } else {
                return rego.getAllowsNeeded();
            }
        }
    }

    public static void removeRegistrationFromPool(String username) {
        if (username != null) {
            registrationPool.remove(username.toLowerCase());
        }
    }

    public static Registration getRegistrationFromPool(String username) {
        if (username == null) {
            return null;
        } else {
            return registrationPool.get(username.toLowerCase());
        }
    }

    public static ConcurrentHashMap<String, ServerData> getServerList() {
        return serverList;
    }
}
