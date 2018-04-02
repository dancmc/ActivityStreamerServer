package activitystreamer.server;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Map;

import activitystreamer.util.JsonCreator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.JSONException;
import org.json.JSONObject;


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

    Connection(Socket socket) throws IOException {
        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        inreader = new BufferedReader(new InputStreamReader(in));
        outwriter = new PrintWriter(out, true);
        this.socket = socket;
        start();
    }

    /*
     * returns true if the message was written, otherwise false
     */
    public boolean writeMsg(String msg) {
        if (open) {
            outwriter.println(msg);
            outwriter.flush();
            return true;

        }
        return false;
    }

    public void closeCon() {
        if (open) {
            log.info("closing connection " + Settings.socketAddress(socket));
            try {
                term = true;
                inreader.close();
                out.close();
            } catch (IOException e) {
                // already closed?
                log.error("received exception closing the connection " + Settings.socketAddress(socket) + ": " + e);
            }
        }
    }

    public void registrationSucceeded() {
        loggedIn = true;

    }


    public void run() {
        try {
            String data;
            while (!term && (data = inreader.readLine()) != null) {
                term = processData(data);
            }
            log.debug("connection closed to " + Settings.socketAddress(socket));
            in.close();

        } catch (IOException e) {
            log.error("connection " + Settings.socketAddress(socket) + " closed with exception: " + e);

        } finally {
            if (type.equals(ConnectionType.CLIENT)) {
                Control.decrementCurrentLoad();
            }
            Control.getInstance().connectionClosed(this);
        }
        open = false;
    }


    private boolean processData(String data) {
        boolean t = false;

//        t = Control.getInstance().process(this, data);

        try {
            JSONObject json = new JSONObject(data);

            String command = json.getString("command");

            switch (command) {
                case "AUTHENTICATE": {
                    String secret = json.getString("secret");
                    if (!secret.equals(Settings.getLocalSecret())) {
                        String error = "Error : Wrong secret";
                        return termConnection(JsonCreator.authenticationFail(error), error);
                    }

                    if (type.equals(ConnectionType.SERVER) && loggedIn) {
                        String error = "Error : Had already authenticated";
                        return termConnection(JsonCreator.invalidMessage(error), error);
                    }

                    type = ConnectionType.SERVER;
                    loggedIn = true;

                    break;
                }

                // failed to connect to rh : log error, close connection, and shutdown server
                case "AUTHENTICATION_FAIL": {
                    String info = json.getString("info");
                    termConnection(null,
                            "failed connection to remote host " + Settings.getRemoteHostname() +
                                    ":" + Settings.getRemotePort() +
                                    " using secret " + Settings.getRemoteSecret() +
                                    " : " + info);
                    Control.getInstance().setTerm(true);
                    break;
                }

                case "LOGIN": {

                    clientId = json.getString("username").toLowerCase();
                    if (!clientId.equals("anonymous")) {
                        String secret = json.getString("secret");

                        // check combination of username and secret, then send success/failure
                        String storedSecret = Control.getUserList().get(clientId);
                        if (storedSecret == null) {
                            String error = "Error : User not registered";
                            return termConnection(JsonCreator.loginFailed(error), error);
                        } else if (!storedSecret.equals(secret)) {
                            String error = "Error : Wrong secret";
                            return termConnection(JsonCreator.loginFailed(error), error);
                        }
                    }

                    String loginMessage = "logged in as user " + clientId;
                    writeMsg(JsonCreator.loginSuccess(loginMessage));
                    log.info(loginMessage);

                    Control.incrementCurrentLoad();
                    type = ConnectionType.CLIENT;
                    loggedIn = true;

                    // now check whether there is another server with lower load, if so, redirect
                    int currentLoad = Control.getCurrentLoad();
                    String newServerId = null;
                    int newServerLoad = Integer.MAX_VALUE;
                    for (ServerData server : Control.getServerList().values()) {
                        int serverLoad = server.getLoad();
                        if (currentLoad - serverLoad >= 2 && serverLoad < newServerLoad) {
                            newServerLoad = serverLoad;
                            newServerId = server.getId();
                        }
                    }

                    if (newServerId != null && !newServerId.equals(clientId)) {
                        ServerData newServer = Control.getServerList().get(newServerId);
                        String newHostName = newServer.getHostname();
                        int newPort = newServer.getPort();
                        log.info("redirected client to " + newHostName + ":" + newPort);
                        return termConnection(JsonCreator.redirect(newHostName, newPort), null);
                    }

                    break;
                }

                case "LOGOUT": {

                    log.info("client " + clientId + " logged out");
                    return termConnection(JsonCreator.logout(), null);

                }

                case "ACTIVITY_MESSAGE": {

                    String username = json.getString("username").toLowerCase();


                    // check that user has logged in & username matches currently logged user
                    if (clientId == null) {
                        String error = "Error : No user logged in";
                        return termConnection(JsonCreator.authenticationFail(error), error);
                    }
                    if (!clientId.equals(username)) {
                        String error = "Error : Username does not match currently logged in user";
                        return termConnection(JsonCreator.authenticationFail(error), error);
                    }

                    if (!username.equals("anonymous")) {
                        String secret = json.getString("secret");

                        // check that user exists
                        String storedSecret = Control.getUserList().get("secret");
                        if(storedSecret==null){
                            String error = "Error : User does not exist";
                            return termConnection(JsonCreator.authenticationFail(error), error);
                        }

                        // check that username and secret match
                        if(!storedSecret.equals(secret)){
                            String error = "Error : Wrong secret";
                            return termConnection(JsonCreator.authenticationFail(error), error);
                        }

                    }

                    return Control.getInstance().process(this, json);

                }

                case "SERVER_ANNOUNCE":{

                    // check that not receiving from an unauthenticated server
                    if(!loggedIn || !type.equals(ConnectionType.SERVER)){
                        String error = "Error : Unauthenticated server";
                        return termConnection(JsonCreator.invalidMessage(error), error);
                    }

                    return Control.getInstance().process(this, json);
                }

                default: {
                    String error = "Error : Unknown command";
                    return termConnection(JsonCreator.invalidMessage(error), error);
                }
            }

        } catch (JSONException e) {
            String error = "JSON parse exception : " + e.getMessage();
            return termConnection(JsonCreator.invalidMessage(error), error);
        }

        return t;
    }

    public boolean termConnection(String messageToClient, String errorMessage) {
        if (messageToClient != null) {
            writeMsg(messageToClient);
        }
        if (errorMessage != null) {
            log.error("connection " + Settings.socketAddress(socket) + " closed with exception : " + errorMessage);
        }
        return true;
    }

    public Socket getSocket() {
        return socket;
    }

    public boolean isOpen() {
        return open;
    }


    public ConnectionType getType() {
        return type;
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
