package com.naturecode.webflux_stream.liverate.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import com.fasterxml.jackson.core.type.TypeReference;
import com.naturecode.webflux_stream.liverate.model.Emitter;
import com.naturecode.webflux_stream.liverate.model.Rate;
import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketExtension;
import com.neovisionaries.ws.client.WebSocketFactory;
import com.neovisionaries.ws.client.WebSocketFrame;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;

// Class representing a session over a WebSocket.
@Component
@Log4j2
public final class WebSocketSession {
  private static WebSocketSession INSTANCE;
  
  private WebSocketSession() {}

  private static String user;
  private static String password;
  private static String clientId;

  @Autowired
  ResourceLoader resourceLoader;

  // Static map used by WebSocketAdapter callbacks to find the associated WebSocketSession object.
  private static Map<WebSocket, WebSocketSession> webSocketSessionMap = new ConcurrentHashMap<WebSocket, WebSocketSession>();
  static Vector<Emitter> listeners = new Vector<Emitter>();

  private String position = "";
  private String appId = "256";
  private String scope = "trapi";
  private WebSocketFactory websocketFactory = new WebSocketFactory();
  private String name = "session1";
  private static WebSocket websocket;
  private String url;
  private static String authToken;
  private static JSONObject authJson = null;
  private static JSONObject serviceJson = null;
  private static AtomicBoolean isLoggedIn = new AtomicBoolean(false);
  private static AtomicBoolean hasListener = new AtomicBoolean(false);
  private static final String authUrl = "https://api.refinitiv.com/auth/oauth2/v1/token";
  private static final String discoveryUrl = "https://api.refinitiv.com/streaming/pricing/v1/";

  public synchronized static WebSocketSession getInstance() {
    if(INSTANCE == null) {
      INSTANCE = new WebSocketSession();
    }
    return INSTANCE;
  }

  public synchronized static void runInstance(String passedUser, String passedPassword, String passedClientId) {
    if(INSTANCE == null) {
      INSTANCE = new WebSocketSession();
    }
    if(!hasListener.get()) {
      user = passedUser;
      password = passedPassword;
      clientId = passedClientId;
      LiverateRunnable runnable = new LiverateRunnable();
      Thread t = new Thread(runnable);
      t.start();

      // first listener subscribes, update hasListener to true
      hasListener = new AtomicBoolean(true);

      String requestJson = "/refinitiv_request/all.json";
      InputStream is = TypeReference.class.getResourceAsStream(requestJson);
      log.info("Start streaming in 5 seconds...");
      try {
        Thread.sleep(5000);
        sendRequest(IOUtils.toString(is, "UTF-8"));
      } catch (InterruptedException e) {
        log.error(e.getMessage());
        e.printStackTrace();
      } catch (JSONException | IOException e) {
        log.error(e.getMessage());
        e.printStackTrace();
      }
    } else {
      log.info("WebSocket is already running");
    }
  }

  public static class LiverateRunnable implements Runnable {
    public LiverateRunnable(){}
    @Override
    public void run() {
      INSTANCE.loginAndDiscover();
    }
  }

  private void loginAndDiscover() {
    boolean hotstandby = false;
    String region = "amer";
    List<String> hostList = new LinkedList<String>();

    try {
      // Connect to Refinitiv Data Platform and authenticate (using our username and password)
      authJson = getAuthenticationInfo(null, authUrl);
      if (authJson == null) {
        log.error("authJson is NULL");
        return;
      }
      authToken = authJson.getString("access_token");

      // Get service information.
      serviceJson = queryServiceDiscovery(discoveryUrl);
      if (serviceJson == null) {
        log.error("serviceJson is NULL");
        return;
      }
        
      // Create a host list based on the retrieved service information.
      // If failing over on disconnect, get an endpoint with two locations.
      // If opening multiple connections, get all endpoints that are in one location.
      JSONArray endpointArray = serviceJson.getJSONArray("services");
      for (int i = 0; i < endpointArray.length(); ++i) {
        JSONObject endpoint = endpointArray.getJSONObject(i);
        if (region.equals("amer")) {
          if (endpoint.getJSONArray("location").getString(0).startsWith("us-") == false)
            continue;
        } else if (region.equals("emea")) {
          if (endpoint.getJSONArray("location").getString(0).startsWith("eu-") == false)
            continue;
        } else if (region.equals("apac")) {
          if (endpoint.getJSONArray("location").getString(0).startsWith("ap-") == false)
            continue;
        }

        if (!hotstandby) {
          if (endpoint.getJSONArray("location").length() == 2) {
            hostList.add(endpoint.getString("endpoint"));
            break;
          }
        } else {
          if (endpoint.getJSONArray("location").length() == 1)
            hostList.add(endpoint.getString("endpoint"));
        }
      }

      // Determine when the access token expires. We will re-authenticate before then.
      int expireTime = Integer.parseInt(authJson.getString("expires_in")); 
      long expire = System.currentTimeMillis() + (expireTime * 800);
      if (hotstandby) {
        if (hostList.size() < 2) {
          log.error("Error: Expected 2 hosts but received " + hostList.size());
        }
      } else {
        if (hostList.size() == 0) {
          log.error("Error: No endpoints in response.");
        }
      }

      // connect to websocket to start streaming
      url = String.format("wss://%s/WebSocket", hostList.get(0));
      connect();

      while (hasListener.get()) {
        // Continue using current token until 80% of initial time before it expires and has listener.
        if(hasListener.get() && System.currentTimeMillis() < expire) {
          // log.info(System.currentTimeMillis() + " - " + expire);
          Thread.sleep(500);
          continue;
        }

        log.info("updating token");

        // re-authenticate, using the refresh token provided in the previous response
        authJson = getAuthenticationInfo(authJson, authUrl);
        if (authJson == null) {
          log.error("authJson is NULL");
          return;
        }

        // If expiration time returned by refresh request is less then initial
        // expiration time, re-authenticate using password
        int refreshingExpireTime = Integer.parseInt(authJson.getString("expires_in"));
        if (refreshingExpireTime != expireTime) {
          log.info("expire time changed from " + expireTime + " sec to " + refreshingExpireTime + " sec; retry with password");
          authJson = getAuthenticationInfo(null, authUrl);
          if (authJson == null) {
            log.error("authJson is NULL");
            return;
          }
          expireTime = Integer.parseInt(authJson.getString("expires_in"));
        }

        // update expire time
        expire = System.currentTimeMillis() + (expireTime * 800);

        // Send the updated access token over our WebSockets.
        updateToken(authJson.getString("access_token"));
      }
      log.info("update token loop break - thread is done");
    } catch (JSONException e) {
      log.error(e.getMessage());
      e.printStackTrace();
    } catch (InterruptedException e) {
      log.error(e.getMessage());
      e.printStackTrace();
    }
  }

  private JSONObject getAuthenticationInfo(JSONObject previousAuthResponseJson, String url) {
    try {
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(new SSLContextBuilder().build());
      HttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
      HttpPost httppost = new HttpPost(url);
      HttpParams httpParams = new BasicHttpParams();

      // Set request parameters.
      List<NameValuePair> params = new ArrayList<NameValuePair>(2);
      params.add(new BasicNameValuePair("client_id", clientId));
      params.add(new BasicNameValuePair("username", user));

      if (previousAuthResponseJson == null) {
        // First time through, send password.
        params.add(new BasicNameValuePair("grant_type", "password"));
        params.add(new BasicNameValuePair("password", password));
        params.add(new BasicNameValuePair("scope", scope));
        params.add(new BasicNameValuePair("takeExclusiveSignOnControl", "true"));
        log.info("Sending authentication request with password to " + url + "...");

      } else {
        // Use the refresh token we got from the last authentication response.
        params.add(new BasicNameValuePair("grant_type", "refresh_token"));
        params.add(new BasicNameValuePair("refresh_token", previousAuthResponseJson.getString("refresh_token")));
        // System.out.println("Sending authentication request with refresh token to " + url + "...");
      }
      httppost.setParams(httpParams);
      httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));

      // Execute and get the response.
      HttpResponse response = httpclient.execute(httppost);
      int statusCode = response.getStatusLine().getStatusCode();
      switch (statusCode) {
        case HttpStatus.SC_OK: // 200
          // Authentication was successful. Deserialize the response and return it.
          JSONObject responseJson = new JSONObject(EntityUtils.toString(response.getEntity()));
          return responseJson;
        case HttpStatus.SC_MOVED_PERMANENTLY: // 301
        case HttpStatus.SC_MOVED_TEMPORARILY: // 302
        case HttpStatus.SC_TEMPORARY_REDIRECT: // 307
        case 308: // 308 HttpStatus.SC_PERMANENT_REDIRECT
          // Perform URL redirect
          log.info("Refinitiv Data Platform authentication HTTP code: " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          Header header = response.getFirstHeader("Location");
          if (header != null) {
            String newHost = header.getValue();
            if (newHost != null) {
              log.info("Perform URL redirect to " + newHost);
              return getAuthenticationInfo(previousAuthResponseJson, newHost);
            }
          }
          return null;
        case HttpStatus.SC_BAD_REQUEST: // 400
        case HttpStatus.SC_UNAUTHORIZED: // 401
          // Retry with username and password
          log.info("Refinitiv Data Platform authentication HTTP code: " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          if (previousAuthResponseJson != null) {
            log.info("Retry with username and password");
            return getAuthenticationInfo(null, authUrl);
          }
          return null;
        case HttpStatus.SC_FORBIDDEN: // 403
        case 451: // 451 Unavailable For Legal Reasons
          // Stop retrying with the request
          log.info("Refinitiv Data Platform authentication HTTP code: " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          log.info("Stop retrying with the request");
          return null;
        default:
          // Retry the request to Refinitiv Data Platform
          log.info("Refinitiv Data Platform authentication HTTP code: " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          log.info("Retry the request to Refinitiv Data Platform");
          return getAuthenticationInfo(previousAuthResponseJson, authUrl);
      }
    } catch (Exception e) {
      log.error("Refinitiv Data Platform authentication failure:");
      e.printStackTrace();
      return null;
    }
  }

  private JSONObject queryServiceDiscovery(String url) {
    try {
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(new SSLContextBuilder().build());
      HttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
      HttpGet httpget = new HttpGet(url + "?transport=websocket");
      HttpParams httpParams = new BasicHttpParams();

      httpget.setParams(httpParams);
      httpget.setHeader("Authorization", "Bearer " + authJson.getString("access_token"));
      log.info("Sending service discovery request to " + url + "...");

      // Execute and get the response.
      HttpResponse response = httpclient.execute(httpget);
      int statusCode = response.getStatusLine().getStatusCode();

      switch (statusCode) {
        case HttpStatus.SC_OK: // 200
          // Service discovery was successful. Deserialize the response and return it.
          JSONObject responseJson = new JSONObject(EntityUtils.toString(response.getEntity()));
          return responseJson;
        case HttpStatus.SC_MOVED_PERMANENTLY: // 301
        case HttpStatus.SC_MOVED_TEMPORARILY: // 302
        case HttpStatus.SC_SEE_OTHER: // 303
        case HttpStatus.SC_TEMPORARY_REDIRECT: // 307
        case 308: // 308 HttpStatus.SC_PERMANENT_REDIRECT
          // Perform URL redirect
          log.info("Refinitiv Data Platform service discovery HTTP code: " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          Header header = response.getFirstHeader("Location");
          if (header != null) {
            String newHost = header.getValue();
            if (newHost != null) {
              log.info("Perform URL redirect to " + newHost);
              return queryServiceDiscovery(newHost);
            }
          }
          return null;
        case HttpStatus.SC_FORBIDDEN: // 403
        case 451: // 451 Unavailable For Legal Reasons
          // Stop retrying with the request
          log.info(" Refinitiv Data Platform service discovery HTTP code: "
              + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          log.info("Stop retrying with the request");
          return null;
        default:
          // Retry the service discovery request
          log.info("Refinitiv Data Platform service discovery HTTP code: " + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          log.info("Retry the service discovery request");
          return queryServiceDiscovery(discoveryUrl);
      }
    } catch (Exception e) {
      log.error("Refinitiv Data Platform service discovery failure:");
      e.printStackTrace();
      return null;
    }
  }

  /** Connect a WebSocket (and reconnect if a previous connection failed). */
  private void connect() {
    if (websocket != null) {
      // Remove websocket from map, and create a new one based on the previous websocket.
      webSocketSessionMap.remove(this);
      try {
        websocket = websocket.recreate();
      } catch (IOException e) {
        log.error(e.getMessage());
        e.printStackTrace();
      }
    } else {
      try {
        position = Inet4Address.getLocalHost().getHostAddress();
      } catch (UnknownHostException e) {
        log.error(e.getMessage());
        // If localhost isn't defined, use 127.0.0.1.
        position = "127.0.0.1/net";
      }

      /* Create new websocket. */
      log.info("Connecting to WebSocket " + url + " for " + name + "...");
      try {
        websocket = websocketFactory.createSocket(url).addProtocol("tr_json2").addListener(new WebSocketAdapter() {
          public void onTextMessage(WebSocket websocket, String message) throws JSONException, IOException {
            WebSocketSession webSocketSession = webSocketSessionMap.get(websocket);
            JSONArray jsonArray = new JSONArray(message);
            for (int i = 0; i < jsonArray.length(); ++i) {
              webSocketSession.processMessage(jsonArray.getJSONObject(i));
            }
          }

          public void onConnected(WebSocket websocket, Map<String, List<String>> headers) throws JSONException, IOException {
            log.info("WebSocket successfully connected for " + name + "!");
            sendLoginRequest(true);
          }

          public void onConnectError(WebSocket websocket, WebSocketException e) {
            log.error("Connect error for " + name + ":" + e);
            reconnect(websocket);
          }

          public void onDisconnected(WebSocket websocket, WebSocketFrame serverCloseFrame, WebSocketFrame clientCloseFrame, boolean closedByServer){
            log.info("WebSocket disconnected for " + name + ".");
            // reconnect(websocket);

            WebSocketSession webSocketSession = webSocketSessionMap.get(websocket);
            webSocketSession.isLoggedIn(false);
          }

          public void reconnect(WebSocket websocket) {
            WebSocketSession webSocketSession = webSocketSessionMap.get(websocket);
            webSocketSession.isLoggedIn(false);
            log.info("Reconnecting to " + name + " in 3 seconds...");

            try {
              Thread.sleep(3000);
            } catch (InterruptedException e) {
              log.error(e.getMessage());
              e.printStackTrace();
            }
            webSocketSession.connect();
          }
        }).addExtension(WebSocketExtension.PERMESSAGE_DEFLATE);
      } catch (IOException e) {
        log.error(e.getMessage());
        e.printStackTrace();
      }
    }
    webSocketSessionMap.put(websocket, this);
    websocket.connectAsynchronously();
  }

  /**
   * Generate a login request from command line data (or defaults) and send Used
   * for both the initial login and subsequent logins that send updated access
   * tokens.
   * 
   * @param authToken
   * @throws JSONException
   * @throws IOException
   */
  private void sendLoginRequest(boolean isFirstLogin) throws JSONException, IOException {
    InputStream is = TypeReference.class.getResourceAsStream("/refinitiv_request/login.json");
    String loginJsonString = IOUtils.toString(is, "UTF-8");
    JSONObject loginJson = new JSONObject(loginJsonString);
    loginJson.getJSONObject("Key").getJSONObject("Elements").put("AuthenticationToken", authToken);
    loginJson.getJSONObject("Key").getJSONObject("Elements").put("ApplicationId", appId);
    loginJson.getJSONObject("Key").getJSONObject("Elements").put("Position", position);

    if (!isFirstLogin) // If this isn't our first login, we don't need another refresh for it.
      loginJson.put("Refresh", false);

    websocket.sendText(loginJson.toString());
  }

  /**
   * Create and send simple Market Price request
   * 
   * @throws JSONException
   * @throws IOException
   */
  public static void sendRequest(String request) throws JSONException, IOException {
    websocket.sendText(request);
  }

  public synchronized void subscribe(Emitter listener) {
    log.info("stream " + listener.getId() + " subscribed...");
    listeners.add(listener);
  }

  public synchronized void unsubscribe(UUID uuid) {
    int removeInd = -1;
    for (int i = 0; i < listeners.size(); i++) {
      if (listeners.get(i).getId().compareTo(uuid) == 0) {
        removeInd = i;
        break;
      }
    }
    if (removeInd >= 0){
      log.info("stream " + listeners.get(removeInd).getId() + " unsubscribed...");
      listeners.remove(removeInd);

      // set hasListener to false to so that we won't send Pong signal, server then kill the websocket conn
      log.info("size:  " + listeners.size());
      if(listeners.size() == 0) {
        log.info("No more listener...");
        hasListener.set(false);
      }
    } else {
      log.error("no stream " + uuid + " found for cancelling" );
    }
  }

  /**
   * Process a message received over the WebSocket
   * 
   * @param messageJson
   * @throws JSONException
   * @throws IOException
   */
  public void processMessage(JSONObject messageJson) throws JSONException, IOException {
    String messageType = messageJson.getString("Type");
    // Only print out non login, update/refresh message
    if (!messageJson.has("Domain") && (messageType.equals("Refresh") || messageType.equals("Update"))) {
      JSONObject currentRate = new JSONObject(messageJson.toString(2));
      String timestamp = currentRate.getJSONObject("Fields").getString("VALUE_TS1");
      double rate = currentRate.getJSONObject("Fields").getDouble("RT_YIELD_1");
      double primaryAct = currentRate.getJSONObject("Fields").getDouble("PRIMACT_1");
      String service = currentRate.getJSONObject("Key").getString("Service");
      String name = currentRate.getJSONObject("Key").getString("Name");
      int duration = Integer.parseInt(name.substring(2, name.indexOf("Y")));

      // send back the reactive change to client who subscribed to a specific rate year
      for (Emitter itr: listeners) {
        if (itr.getDuration() == duration) {
          Rate liveRate = new Rate(rate, primaryAct, name, service, timestamp, itr.getId().toString());
          log.info("-> " + name + " - " + rate + " - " + timestamp);
          itr.getSink().next(liveRate);
        }
      }
    }
    switch (messageType) {
      case "Refresh":
      case "Status":
        if (messageJson.has("Domain")) {
          String messageDomain = messageJson.getString("Domain");
          if (messageDomain.equals("Login")) {
            // Check message state to see if login succeeded. If so, send item request. Otherwise stop.
            JSONObject messageState = messageJson.optJSONObject("State");
            if (messageState != null) {
              if (!messageState.getString("Stream").equals("Open") || !messageState.getString("Data").equals("Ok")) {
                log.error("Login failed.");
              }

              // Login succeeded, send item request.
              isLoggedIn(true);
              // sendRequest();
            }
          }
        }
        break;
      case "Ping":
        // only send heartbeat signal back when there's still listener being subscribed
        if(hasListener.get()) {
          websocket.sendText("{\"Type\":\"Pong\"}");
        } else {
          log.info("No more listener - won't return Pong signal");
        }
        break;
      default:
        break;
    }
  }

  /**
   * Send a login request on the websocket that includes our updated access token.
   */
  private void updateToken(String updatedAuthToken) {
    authToken = updatedAuthToken;

    // Websocket not connected or logged in yet. Initial login will include the access token.
    if (!isLoggedIn.get() || !hasListener.get()) {
      log.info("not logged in");
      return;
    }
    
    try {
      log.info("resend LoginRequest");
      sendLoginRequest(false);
    } catch (JSONException | IOException e) {
      log.error(e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Mark whether we are connected and logged in so that the updateToken method
   * knows whether or not to reissue the login.
   */
  public synchronized void isLoggedIn(boolean flip) {
    isLoggedIn.set(flip);
  }
}