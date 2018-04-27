package activitystreamer.server;


import activitystreamer.server.aux.ServerData;
import activitystreamer.util.JsonCreator;
import activitystreamer.util.Settings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.Socket;


public class Connection extends Thread {
    private static final Logger log = LogManager.getLogger();

    private Socket socket;
    private DataInputStream in;
    private DataOutputStream out;
    private BufferedReader inreader;
    private PrintWriter outwriter;

    private boolean open = true;
    private boolean term = false;
    private boolean loggedIn = false; // login for client, auth for server
    private String clientId = null; // if the connection is to a client and has attempted to log in
    private ConnectionType type = null;
    private String connectionId = null; // can be either server or client id

    // just for debugging
    public long timeCreated;


    Connection(Socket socket, boolean outgoing) throws IOException {

        // if incoming, have to wait for auth/login messages to determine
        if(outgoing){
            setType(Connection.ConnectionType.SERVER);
            setLoggedIn(true);
        }

        timeCreated = System.currentTimeMillis();

        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        inreader = new BufferedReader(new InputStreamReader(in));
        outwriter = new PrintWriter(out, true);
        this.socket = socket;
        start();
    }

    /**
     * Write to output stream of socket, PrintWriter implementation is threadsafe
     *
     * @param msg string to be written
     * @return true if connection is open and attempted write, but doesn't necessarily guaranteed msg sent
     */
    public synchronized boolean writeMsg(String msg) {
        if (open) {
            outwriter.println(msg);
            outwriter.flush();
            return true;
        }
        return false;
    }

    /**
     * Close the connection and cleanup streams
     */
    public void closeCon() {
        if (open) {
            log.info("INFO - closing connection " + Settings.socketAddress(socket));
            try {
                term = true;
                open = false;
                socket.close();
            } catch (IOException e) {
                // already closed?
                log.error("ERROR - exception closing connection " + Settings.socketAddress(socket) + " : " + e);
            }
        }
    }

    /**
     * Run main thread loop
     */
    public void run() {
        try {
            String data;
            while (!term && (data = inreader.readLine()) != null) {
                // this is probably a terrible way of making sure closeCon() not overwritten if processData is underway
                term = processData(data) || term;
            }
            socket.close();
            log.debug("INFO - connection to " + Settings.socketAddress(socket) + " closed");
        } catch (IOException e) {
            log.error("ERROR - connection to " + Settings.socketAddress(socket) + " closed with exception : " + e);

        } finally {
            if (isClient()) {
                Control.decrementCurrentLoad();
            }
            Control.getInstance().connectionClosed(this);
        }
        open = false;
        outwriter.close();
    }


    /**
     * Processing of data received in individual connection. Calls synchronised Control method if broadcast required
     *
     * @param data data string received
     * @return true if connection should close based on data received
     */
    private boolean processData(String data) {


        try {
            JSONObject json = new JSONObject(data);

            String command = json.getString("command");

            switch (command) {
                case "AUTHENTICATE": {
                    String secret = json.getString("secret");
                    if (!secret.equals(Settings.getSecret())) {
                        String error = "wrong secret";
                        return termConnection(JsonCreator.authenticationFail(error), "AUTHENTICATE - "+error);
                    }

                    if (isServer() && loggedIn) {
                        String error = "already authenticated";
                        return termConnection(JsonCreator.invalidMessage(error), "AUTHENTICATE - "+error);
                    }

                    type = ConnectionType.SERVER;
                    loggedIn = true;

                    log.info("AUTHENTICATE - successfully authenticated server");
                    break;
                }

                case "INVALID_MESSAGE":{
                    String info  = json.getString("info");
                    return termConnection(null, "INVALID_MESSAGE : "+info);
                }

                // failed to connect to rh : log error, close connection
                case "AUTHENTICATION_FAIL": {
                    String info = json.getString("info");
                    return termConnection(null,
                            "AUTHENTICATION_FAIL - remote host " + Settings.socketAddress(socket)+
                                    " using secret " + Settings.getSecret() +
                                    " : " + info);
                }

                case "LOGIN": {

                    String username = json.getString("username");

                    // if username is not anonymous, do some checks
                    if (!username.equals("anonymous")) {
                        String secret = json.getString("secret");

                        // validate combination of username and secret & send failure if incorrect
                        if (!Control.userExists(username)) {
                            String error = "user not registered";
                            return termConnection(JsonCreator.loginFailed(error), "LOGIN - "+error);
                        } else if (!secret.equals(Control.getSecretForUser(username))) {
                            String error = "wrong secret";
                            return termConnection(JsonCreator.loginFailed(error), "LOGIN - "+error);
                        }
                    }

                    // otherwise send success
                    clientId = username;
                    String loginMessage = "logged in as user " + clientId;
                    writeMsg(JsonCreator.loginSuccess(loginMessage));
                    log.info("LOGIN_SUCCESS - "+loginMessage);

                    if(!loggedIn) {
                        Control.incrementCurrentLoad();
                    }
                    type = ConnectionType.CLIENT;
                    loggedIn = true;

                    // now check whether there is another server with lower load, if so, redirect
                    int currentLoad = Control.getCurrentLoad();
                    String newServerId = null;
                    int newServerLoad = Integer.MAX_VALUE;

                    // find a server that is at least 2 load lower
                    for (ServerData server : Control.getServerList().values()) {
                        int serverLoad = server.getLoad();
                        if (currentLoad - serverLoad >= 2 && serverLoad < newServerLoad) {
                            newServerId = server.getId();
                            newServerLoad = serverLoad;
                        }
                    }

                    if (newServerId != null) {
                        ServerData newServer = Control.getServerList().get(newServerId);
                        String newHostName = newServer.getHostname();
                        int newPort = newServer.getPort();
                        log.info("REDIRECT -  to " + newHostName + ":" + newPort);
                        return termConnection(JsonCreator.redirect(newHostName, newPort), null);
                    }

                    break;
                }

                case "LOGOUT": {

                    log.info("LOGOUT - client " + clientId + " logged out");
                    return termConnection(null, null);

                }

                case "ACTIVITY_MESSAGE": {

                    String username = json.getString("username");


                    // check that user has logged in & is client
                    if (!isLoggedIn() || !isClient()) {
                        String error = "no user client logged in";
                        return termConnection(JsonCreator.authenticationFail(error),"ACTIVITY_MESSAGE - " +error);
                    }

                    // check that username matches currently logged user
                    if (!username.equals(clientId)) {
                        String error = "username does not match currently logged in user";
                        return termConnection(JsonCreator.authenticationFail(error), "ACTIVITY_MESSAGE - " +error);
                    }

                    // if not anonymous, check that secret is correct for username
                    if (!username.equals("anonymous")) {
                        String secret = json.getString("secret");

                        // check that user exists
                        if(!Control.userExists(username)){
                            String error = "user does not exist";
                            return termConnection(JsonCreator.authenticationFail(error), "ACTIVITY_MESSAGE - " +error);
                        }

                        // check that username and secret match
                        String storedSecret = Control.getSecretForUser(username);
                        if(!storedSecret.equals(secret)){
                            String error = "wrong secret";
                            return termConnection(JsonCreator.authenticationFail(error), "ACTIVITY_MESSAGE - " +error);
                        }

                    }

                    return Control.getInstance().process(this, json);

                }

                case "SERVER_ANNOUNCE":{

                    // check that not receiving from an unauthenticated server
                    if(!loggedIn || !isServer()){
                        String error = "unauthenticated server";
                        return termConnection(JsonCreator.invalidMessage(error), "SERVER_ANNOUNCE - "+error);
                    }

                    return Control.getInstance().process(this, json);
                }

                case "ACTIVITY_BROADCAST": {

                    // check it has an activity object
                    JSONObject activity = json.getJSONObject("activity");

                    // check that activity object is processed
                    // apparently unnecessary according to discussion board & test server behaviour
//                    if(!activity.has("authenticated_user")){
//                        String error = "activity object is not properly processed";
//                        return termConnection(JsonCreator.invalidMessage(error), "ACTIVITY_BROADCAST - "+error);
//                    }

                    return Control.getInstance().process(this, json);
                }

                case "REGISTER": {
                    return Control.getInstance().process(this, json);
                }

                case "LOCK_REQUEST": {
                    return Control.getInstance().process(this, json);
                }

                case "LOCK_DENIED": {
                    return Control.getInstance().process(this, json);
                }

                case "LOCK_ALLOWED": {
                    return Control.getInstance().process(this, json);
                }


                default: {
                    String error = "Error : Unknown command";
                    return termConnection(JsonCreator.invalidMessage(error), error);
                }
            }

        } catch (JSONException e) {
            String error = "JSON parse exception : " + e.getMessage();
            return termConnection(JsonCreator.invalidMessage(error), "INVALID_MESSAGE - "+error);
        }

        return false;
    }

    /**
     * Utility method that writes any outgoing messages or logs before returning true to indicate connection should end
     *
     * @param messageToClient client message to write to socket
     * @param errorMessage error message to log
     * @return true, passed to thread loop to end connection
     */
    public boolean termConnection(String messageToClient, String errorMessage) {
        if (messageToClient != null) {
            writeMsg(messageToClient);
        }
        if (errorMessage != null) {
            log.error(errorMessage);
        }
        return true;
    }


    public Socket getSocket() {
        return socket;
    }

    public boolean isOpen() {
        return open;
    }


    public boolean isClient(){
        return type!=null && type.equals(ConnectionType.CLIENT);
    }

    public boolean isServer(){
        return type!=null && type.equals(ConnectionType.SERVER);
    }

    public void setType(ConnectionType type) {
        this.type = type;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public boolean isLoggedIn() {
        return loggedIn;
    }

    public void setLoggedIn(boolean loggedIn) {
        this.loggedIn = loggedIn;
    }

    public String getClientId() {
        return clientId;
    }

    public enum ConnectionType {
        CLIENT, SERVER
    }
}
