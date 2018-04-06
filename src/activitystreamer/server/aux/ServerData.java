package activitystreamer.server.aux;


/**
 * Data class to hold data about other servers
 */
public class ServerData {
    private String id;
    private int load;
    private String hostname;
    private int port;

    public ServerData(String id, int load, String hostname, int port){
        this.id = id;
        this.load = load;
        this.hostname = hostname;
        this.port = port;
    }

    public String getId() {
        return id;
    }

    public void setLoad(int load) {
        this.load = load;
    }

    public int getLoad() {
        return load;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }
}
