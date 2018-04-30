package activitystreamer.util;

import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.SecureRandom;

/**
 * Project : Activity Streamer Server
 * Author : Daniel Chan (mchan@student.unimelb.edu.au)
 * Date : 22 Mar 2018
 */

public class Settings {
    private static final Logger log = LogManager.getLogger();
    private static SecureRandom random = new SecureRandom();

    private static String serverId = null;
    private static String localHostname = "localhost";
    private static int localPort = 3780;
    private static String secret = null;
    private static String remoteHostname = null;
    private static int remotePort = 3780;


    private static int activityInterval = 5000; // milliseconds

    private static void help(Options options){
        String header = "An ActivityStream Server for Unimelb COMP90015\n\n";
        String footer = "\ncontact mchan@student.unimelb.edu.au for issues.";
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ActivityStreamer.Server", header, options, footer, true);
        System.exit(-1);
    }


    public static int getLocalPort() {
        return localPort;
    }

    public static void setLocalPort(int localPort) {
        if (localPort < 0 || localPort > 65535) {
            log.error("supplied port " + localPort + " is out of range, using " + getLocalPort());
        } else {
            Settings.localPort = localPort;
        }
    }

    public static int getRemotePort() {
        return remotePort;
    }

    public static void setRemotePort(int remotePort) {
        if (remotePort < 0 || remotePort > 65535) {
            log.error("supplied port " + remotePort + " is out of range, using " + getRemotePort());
        } else {
            Settings.remotePort = remotePort;
        }
    }

    public static String getRemoteHostname() {
        return remoteHostname;
    }

    public static void setRemoteHostname(String remoteHostname) {
        Settings.remoteHostname = remoteHostname;
    }

    public static int getActivityInterval() {
        return activityInterval;
    }

    public static void setActivityInterval(int activityInterval) {
        Settings.activityInterval = activityInterval;
    }

    public static String getServerId() {
        return serverId;
    }

    public static void setServerId(String serverId) {
        Settings.serverId = serverId;
    }

    public static String getSecret() {
        return secret;
    }

    public static void setSecret(String secret) {
        Settings.secret = secret;
    }

    public static String getLocalHostname() {
        return localHostname;
    }

    public static void setLocalHostname(String localHostname) {
        Settings.localHostname = localHostname;
    }


    /*
     * some general helper functions
     */

    public static String socketAddress(Socket socket) {
        return socket.getInetAddress() + ":" + socket.getPort();
    }

    public static String nextSecret() {
        return new BigInteger(130, random).toString(32);
    }


    public static void parseArguments(String[] args){
        log.info("reading command line options");

        Options options = new Options();
        options.addOption("lh",true,"local hostname");
        options.addOption("lp",true,"local port number");
        options.addOption("rh",true,"remote hostname");
        options.addOption("rp",true,"remote port number");
        options.addOption("a",true,"activity interval in milliseconds");
        options.addOption("s",true,"remote secret for the server to use");


        // build the parser
        CommandLineParser parser = new DefaultParser();

        CommandLine cmd = null;
        try {
            cmd = parser.parse( options, args);
        } catch (ParseException e1) {
            help(options);
        }

        // parse all the options

        try {
            setLocalHostname(InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            log.warn("failed to get localhost IP address");
        }

        if(cmd.hasOption("lh")){
            setLocalHostname(cmd.getOptionValue("lh"));
        }

        if(cmd.hasOption("lp")){
            try{
                int port = Integer.parseInt(cmd.getOptionValue("lp"));
                setLocalPort(port);
            } catch (NumberFormatException e){
                log.info("-lp requires a port number, parsed: "+cmd.getOptionValue("lp"));
                help(options);
            }
        }


        if(cmd.hasOption("rh")){
            Settings.setRemoteHostname(cmd.getOptionValue("rh"));
        }

        if(cmd.hasOption("rp")){
            try{
                int port = Integer.parseInt(cmd.getOptionValue("rp"));
                setRemotePort(port);
            } catch (NumberFormatException e){
                log.error("-rp requires a port number, parsed: "+cmd.getOptionValue("rp"));
                help(options);
            }
        }

        if(cmd.hasOption("s")){
            setSecret(cmd.getOptionValue("s"));
        } else{
            // generate the secret and output it
            Settings.setSecret(Settings.nextSecret());
        }
        log.info("secret : " + Settings.getSecret());

        if(cmd.hasOption("a")){
            try{
                int a = Integer.parseInt(cmd.getOptionValue("a"));
                setActivityInterval(a);
            } catch (NumberFormatException e){
                log.error("-a requires a number in milliseconds, parsed: "+cmd.getOptionValue("a"));
                help(options);
            }
        }


    }

}
