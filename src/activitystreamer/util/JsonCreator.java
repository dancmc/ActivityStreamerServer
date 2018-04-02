package activitystreamer.util;


import org.json.JSONObject;

import java.util.Set;

public class JsonCreator {

    public static JSONObject baseJson(String command){
        return new JSONObject().put("command", command);
    }

    public static String authenticate(String secret){
        JSONObject j = baseJson("AUTHENTICATE");
        j.put("secret", secret);
        return j.toString();
    }

    public static String invalidMessage(String info){
        JSONObject j = baseJson("INVALID_MESSAGE");
        j.put("info", info);
        return j.toString();
    }

    public static String authenticationFail(String info){
        JSONObject j = baseJson("AUTHENTICATION_FAIL");
        j.put("info", info);
        return j.toString();
    }

    public static String login(String username, String secret){
        JSONObject j = baseJson("LOGIN");
        j.put("username", username);
        j.put("secret", secret);
        return j.toString();
    }

    public static String loginSuccess(String info){
        JSONObject j = baseJson("LOGIN_SUCCESS");
        j.put("info", info);
        return j.toString();
    }

    public static String redirect(String hostname, int port){
        JSONObject j = baseJson("REDIRECT");
        j.put("hostname", hostname);
        j.put("port", port);
        return j.toString();
    }

    public static String loginFailed(String info){
        JSONObject j = baseJson("LOGIN_FAILED");
        j.put("info", info);
        return j.toString();
    }

    public static String logout(){
        return baseJson("LOGOUT").toString();
    }

    public static String activityMessage(String username, String secret, JSONObject activity){
        JSONObject j = baseJson("ACTIVITY_MESSAGE");
        j.put("username", username);
        j.put("secret", secret);
        j.put("activity", activity);
        return j.toString();
    }

    public static String serverAnnounce(String id, int load, String hostname, int port){
        JSONObject j = baseJson("SERVER_ANNOUNCE");
        j.put("id", id);
        j.put("load", load);
        j.put("hostname", hostname);
        j.put("port", port);
        return j.toString();
    }

    public static String activityBroadcast(JSONObject activity){
        JSONObject j = baseJson("ACTIVITY_BROADCAST");
        j.put("activity", activity);
        return j.toString();
    }

    public static String register(String username, String secret){
        JSONObject j = baseJson("REGISTER");
        j.put("username", username);
        j.put("secret", secret);
        return j.toString();
    }

    public static String registerFailed(String info){
        JSONObject j = baseJson("REGISTER_FAILED");
        j.put("info", info);
        return j.toString();
    }

    public static String registerSuccess(String info){
        JSONObject j = baseJson("REGISTER_SUCCESS");
        j.put("info", info);
        return j.toString();
    }

    public static String lockRequest(String username, String secret){
        JSONObject j = baseJson("LOCK_REQUEST");
        j.put("username", username);
        j.put("secret", secret);
        return j.toString();
    }

    public static String lockDenied(String username, String secret){
        JSONObject j = baseJson("LOCK_DENIED");
        j.put("username", username);
        j.put("secret", secret);
        return j.toString();
    }

    public static String lockAllowed(String username, String secret){
        JSONObject j = baseJson("LOCK_ALLOWED");
        j.put("username", username);
        j.put("secret", secret);
        return j.toString();
    }

    public static JSONObject processActivityObject(JSONObject activity, String username){
        return activity.put("authenticated_user", username);
    }

//    public static boolean validateMessage(JSONObject message, JSONObject model){
//
//        boolean result = true;
//        Set<String>  modelKeys = model.keySet();
//        Set<String> messageKeys = message.keySet();
//
//        for(String key : modelKeys){
//            if(!messageKeys.contains(key)){
//
//            }
//        }
//
//    }

}
