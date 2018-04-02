package activitystreamer.server;


public class Registration {

    private Connection connection;
    private String username;
    private String secret;
    private int allowsNeeded;

    public Registration(Connection connection, String username, String secret, int allowsNeeded){
        this.connection = connection;
        this.username = username;
        this.secret = secret;
        this.allowsNeeded = allowsNeeded;
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

    public int getAllowsNeeded() {
        return allowsNeeded;
    }

    public void setAllowsNeeded(int allowsNeeded) {
        this.allowsNeeded = allowsNeeded;
    }
}
