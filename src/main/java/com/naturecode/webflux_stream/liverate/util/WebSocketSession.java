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
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.FluxSink;
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
  public static Map<WebSocket, WebSocketSession> webSocketSessionMap = new ConcurrentHashMap<WebSocket, WebSocketSession>();

  // passing the flux sink for streaming
  // FluxSink<Rate> sink;
  Vector<Emitter> listeners = new Vector<Emitter>();

  private String position = "";
  private String appId = "256";
  private String scope = "trapi";
  private WebSocketFactory websocketFactory = new WebSocketFactory();
  private String name = "session1";
  private WebSocket websocket;
  private String url;
  private String authToken;
  private JSONObject authJson = null;
  private JSONObject serviceJson = null;
  boolean isLoggedIn = false;
  private static final String authUrl = "https://api.refinitiv.com/auth/oauth2/v1/token";
  private static final String discoveryUrl = "https://api.refinitiv.com/streaming/pricing/v1/";

  public void run() {
    INSTANCE.loginAndDiscover();
  }

  public synchronized static WebSocketSession getInstance() {
    if(INSTANCE == null) {
      INSTANCE = new WebSocketSession();
    }
    return INSTANCE;
  }

  public synchronized static void runInstance(String passedUser, String passedPassword, String passedClientId) {
    if(INSTANCE == null) {
      INSTANCE = new WebSocketSession();
      user = passedUser;
      password = passedPassword;
      clientId = passedClientId;
      LiverateRunnable runnable = new LiverateRunnable();
      Thread t = new Thread(runnable);
      t.start();
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
      this.authJson = getAuthenticationInfo(null, authUrl);
      if (authJson == null) {
        log.error("authJson is NULL");
        // return;
      }
      authToken = authJson.getString("access_token");

      // Get service information.
      serviceJson = queryServiceDiscovery(discoveryUrl);
      if (serviceJson == null) {
        log.error("serviceJson is NULL");
        // return;
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

      while (true) {
        // log.info("in while loop");
        // Continue using current token until 90% of initial time before it expires.
        Thread.sleep(expireTime * 800); // The value 900 means 90% of expireTime in milliseconds
        log.info("Time 80%: " + new Timestamp(System.currentTimeMillis()));

        // Connect to Refinitiv Data Platform and re-authenticate, using the refresh
        // token provided in the previous response
        authJson = getAuthenticationInfo(authJson, authUrl);
        if (authJson == null) {
          log.error("authJson is NULL");
        }
        // log.info("auth: " + authJson.toString());

        // If expiration time returned by refresh request is less then initial
        // expiration time, re-authenticate using password
        int refreshingExpireTime = Integer.parseInt(authJson.getString("expires_in"));
        // log.info("new Time: " + refreshingExpireTime);
        if (refreshingExpireTime != expireTime) {
          log.info("expire time changed from " + expireTime + " sec to " + refreshingExpireTime + " sec; retry with password");
          authJson = getAuthenticationInfo(null, authUrl);
          if (authJson == null) {
            log.error("authJson is NULL");
            // return;
          }
          expireTime = Integer.parseInt(authJson.getString("expires_in"));
        }

        // Send the updated access token over our WebSockets.
        updateToken(authJson.getString("access_token"));
      }
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
          // System.out.println("Refinitiv Data Platform Authentication succeeded. RECEIVED:");
          // System.out.println(responseJson.toString(2));
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

          public void onDisconnected(WebSocket websocket, WebSocketFrame serverCloseFrame,
              WebSocketFrame clientCloseFrame, boolean closedByServer){
            log.info("WebSocket disconnected for " + name + ".");
            // reconnect(websocket);
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
        // System.exit(1);
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
    // String loginJsonString = "{\"ID\":1,\"Domain\":\"Login\",\"Key\":{\"Elements\":{\"ApplicationId\":\"\",\"Position\":\"\",\"AuthenticationToken\":\"\"},\"NameType\":\"AuthnToken\"}}";
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
  public void sendRequest(String request) throws JSONException, IOException {
    websocket.sendText(request);
  }

  public void subscribe(Emitter listener) {
    log.info("stream " + listener.getId() + " subscribed...");
    listeners.add(listener);
  }

  public void unsubscribe(Emitter listener) {
    int removeInd = -1;
    for (int i = 0; i < listeners.size(); i++) {
      if (listeners.get(i).getId().compareTo(listener.getId()) == 0) {
        removeInd = i;
      }
    }
    if (removeInd >= 0){
      log.info("stream " + listener.getId() + " unsubscribed...");
      listeners.remove(removeInd);
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
      log.info("--> " + messageJson.toString(2));
      JSONObject currentRate = new JSONObject(messageJson.toString(2));
      String timestamp = currentRate.getJSONObject("Fields").getString("VALUE_TS1");
      double rate = currentRate.getJSONObject("Fields").getDouble("RT_YIELD_1");
      double primaryAct = currentRate.getJSONObject("Fields").getDouble("PRIMACT_1");
      String service = currentRate.getJSONObject("Key").getString("Service");
      String name = currentRate.getJSONObject("Key").getString("Name");
      int duration = Integer.parseInt(name.substring(2, name.indexOf("Y")));
      Rate liveRate = new Rate(rate, primaryAct, name, service, timestamp);

      for (Emitter itr: listeners) {
        if (itr.getDuration() == duration) {
          itr.getSink().next(liveRate);
        }
      }

      // send back the reactive change to client
      // sink.next(liveRate);
    }
    switch (messageType) {
      case "Refresh":
      case "Status":
        if (messageJson.has("Domain")) {
          String messageDomain = messageJson.getString("Domain");
          if (messageDomain.equals("Login")) {
            // log.info("=> " + messageJson.toString(2));
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
        String pongJsonString = "{\"Type\":\"Pong\"}";
        // JSONObject pongJson = new JSONObject(pongJsonString);
        // log.info("SENT on " + name + ": \n" + pongJson.toString(2));
        websocket.sendText(pongJsonString);
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
    if (!isLoggedIn)
      return;

      try {
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
  public synchronized void isLoggedIn(boolean isLoggedIn) {
    this.isLoggedIn = isLoggedIn;
  }
}