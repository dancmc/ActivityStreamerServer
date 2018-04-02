package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import activitystreamer.util.JsonCreator;
import com.sun.tools.javac.util.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;
import org.json.JSONException;
import org.json.JSONObject;

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static Control control = null;
	private static AtomicInteger currentLoad = new AtomicInteger(0);

	private static ArrayList<Connection> connections;
	private static boolean term=false;
	private static Listener listener;

	private static ConcurrentHashMap<String, String> userList= new ConcurrentHashMap<>();
	private static ConcurrentHashMap<String, ServerData> serverList= new ConcurrentHashMap<>();
	private static HashMap<String, Registration> registrationPool = new HashMap<>();




	// only allow one instance of Control
	public static Control getInstance() {
		if(control==null){
			control=new Control();
		} 
		return control;
	}

	private Control() {

		// generate server id and output it
		Settings.setServerId(Settings.nextSecret());
		log.info("server id : " + Settings.getServerId());

		// generate the local secret and output it
		Settings.setLocalSecret(Settings.nextSecret());
		log.info("local secret : " + Settings.getLocalSecret());

		// initialize the connections array
		connections = new ArrayList<Connection>();

		// start a listener
		try {
			listener = new Listener();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: "+e1);
			System.exit(-1);
		}	
	}
	
	public void initiateConnection(){
		// make a connection to another server if remote hostname is supplied
		if(Settings.getRemoteHostname()!=null){
			try {
				Connection outgoingConn = outgoingConnection(new Socket(Settings.getRemoteHostname(),Settings.getRemotePort()));
				boolean writeResult = outgoingConn.writeMsg(JsonCreator.authenticate(Settings.getRemoteSecret()));
				outgoingConn.setType(Connection.ConnectionType.SERVER);
				outgoingConn.setLoggedIn(true);

				if(!writeResult){
					throw new IOException("connection closed");
				}

			} catch (IOException e) {
				log.error("failed to make connection to "+Settings.getRemoteHostname()+":"+Settings.getRemotePort()+" :"+e);
				System.exit(-1);
			}
		}
	}
	
	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	public synchronized boolean process(Connection con,JSONObject json){

		try {
			String command = json.getString("command");

			switch(command){
				case "ACTIVITY_MESSAGE":{

					JSONObject processedActivityObject = json
							.getJSONObject("activity")
							.put("authenticated_user", con.getClientId());

					for(Connection connection :connections){
						if(connection!=con){
							connection.writeMsg(JsonCreator.activityBroadcast(processedActivityObject));
						}
					}


					break;
				}

				case "SERVER_ANNOUNCE":{

					// assuming no servers quit or crash
					// update server list then forward
					String id = json.getString("id");
					int load = json.getInt("load");
					String hostname = json.getString("hostname");
					int port  = json.getInt("port");

					ServerData server = serverList.get(id);
					if(server==null) {
						serverList.put(id, new ServerData(id, load, hostname, port));
					} else {
						server.setLoad(load);
					}

					for(Connection connection :connections){
						if(connection.getType().equals(Connection.ConnectionType.SERVER) && connection.isLoggedIn()){
							connection.writeMsg(json.toString());
						}
					}

					break;

				}

				default: {
					String error = "Error : Unknown command";
					return con.termConnection(JsonCreator.invalidMessage(error), error);
				}
			}

		} catch (JSONException e){
			String error = "JSON parse exception : " + e.getMessage();
			return con.termConnection(JsonCreator.invalidMessage(error), error);
		}


		return false;
	}
	
	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con){
		if(!term) connections.remove(con);
	}

	// TODO only counting client connections into load
	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException{
		log.debug("incomming connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}
	
	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException{
		log.debug("outgoing connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}
	
	@Override
	public void run(){
		log.info("using activity interval of "+Settings.getActivityInterval()+" milliseconds");
		while(!term){
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}
			if(!term){
				log.debug("doing activity");
				term=doActivity();
			}
			
		}
		log.info("closing "+connections.size()+" connections");
		// clean up
		for(Connection connection : connections){
			connection.closeCon();
		}
		listener.setTerm(true);
	}
	
	public boolean doActivity(){
		return false;
	}
	
	public final void setTerm(boolean t){
		term=t;
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

	public final ArrayList<Connection> getConnections() {
		return connections;
	}

	public static ConcurrentHashMap<String, String> getUserList() {
		return userList;
	}

	public static ConcurrentHashMap<String, ServerData> getServerList() {
		return serverList;
	}
}
