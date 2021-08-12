package com.naturecode.webflux_stream.liverate.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.naturecode.webflux_stream.liverate.model.Rate;

//|-----------------------------------------------------------------------------
//|            This source code is provided under the Apache 2.0 license      --
//|  and is provided AS IS with no warranty or guarantee of fit for purpose.  --
//|                See the project's LICENSE.md for details.                  --
//|            Copyright (C) 2018-2020 Refinitiv. All rights reserved.        --
//|-----------------------------------------------------------------------------

import com.neovisionaries.ws.client.WebSocket;
import com.neovisionaries.ws.client.WebSocketAdapter;
import com.neovisionaries.ws.client.WebSocketException;
import com.neovisionaries.ws.client.WebSocketExtension;
import com.neovisionaries.ws.client.WebSocketFactory;
import com.neovisionaries.ws.client.WebSocketFrame;

import java.io.IOException;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.json.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import lombok.extern.log4j.Log4j2;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import org.apache.http.*;
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
import org.apache.commons.io.IOUtils;

/*
 * This example demonstrates authenticating via Refinitiv Data Platform, using an
 * authentication token to discover Refinitiv Real-Time service endpoint, and
 * using the endpoint and authentitcation to retrieve market content.
 *
 * This example maintains a session by proactively renewing the authentication
 * token before expiration.
 *
 * This example can run with optional hotstandby support. Without this support, the application
 * will use a load-balanced interface with two hosts behind the load balancer. With hot standly
 * support, the application will access two hosts and display the data (should be identical) from
 * each of the hosts.
 *
 * It performs the following steps:
 * - Authenticating via HTTP Post request to Refinitiv Data Platform
 * - Retrieving service endpoints from Service Discovery via HTTP Get request,
 *   using the token retrieved from Refinitiv Data Platform
 * - Opening a WebSocket (or two, if the --hotstandby option is specified) to
 *   a Refinitiv Real-Time Service endpoint, as retrieved from Service Discovery
 * - Sending Login into the Real-Time Service using the token retrieved
 *   from Refinitiv Data Platform.
 * - Requesting market-price content.
 * - Printing the response content.
 * - Periodically proactively re-authenticating to Refinitiv Data Platform, and
 *   providing the updated token to the Real-Time endpoint before token expiration.
 */

 @Log4j2
public class RefinitivRate {
  public static String user = "root";
  public static String clientid = "";
  public static String password = "";
  // public static String ric = "/TRI.N";
  // public static String service = "ELEKTRON_DD";
  public static String position = "";
  public static String appId = "256";
  public static String authUrl = "https://api.refinitiv.com/auth/oauth2/v1/token";
  public static String discoveryUrl = "https://api.refinitiv.com/streaming/pricing/v1/";
  public static String scope = "trapi";
  public static JSONObject authJson = null;
  public static JSONObject serviceJson = null;
  public static List<String> hostList = new LinkedList<String>();
  public static WebSocketFactory websocketFactory = new WebSocketFactory();
  public static WebSocketSession webSocketSession1 = null;
  public static WebSocketSession webSocketSession2 = null;
  public static boolean hotstandby = false;
  public static String region = "amer";
  
  // Class representing a session over a WebSocket.
  public static class WebSocketSession {
    /** Name to use when printing messages sent/received over this WebSocket. */
    String _name;

    /** Current WebSocket associated with this session. */
    WebSocket _websocket;

    /** URL to connect the websocket to. */
    String _url;

    /** Copy of the current authentication token. */
    String _authToken;

    /** Whether the session has successfully logged in. */
    boolean _isLoggedIn = false;

    // The payload being sent inside the request
    String _request;

    // passing the flux sink for streaming
    FluxSink<Rate> _sink;

    // number of year rate
    int _number;

    @Autowired
    ResourceLoader resourceLoader;

    /**
     * Static map used by WebSocketAdapter callbacks to find the associated WebSocketSession object.
     */
    public static Map<WebSocket, WebSocketSession> webSocketSessionMap = new ConcurrentHashMap<WebSocket, WebSocketSession>();

    public WebSocketSession(FluxSink<Rate> sink, int number, String name, String host, String authToken) {
      _name = name;
      _url = String.format("wss://%s/WebSocket", host);
      _authToken = authToken;
      _sink = sink;
      _number = number;
      try {
        String rateYearResource = "refinitiv_request/10y.json";
        switch (number) {
          case 2:
            rateYearResource = "refinitiv_request/2y.json";
            break;
          case 3:
            rateYearResource = "refinitiv_request/3y.json";
            break;
          case 5:
            rateYearResource = "refinitiv_request/5y.json";
            break;
          case 7:
            rateYearResource = "refinitiv_request/7y.json";
            break;
          case 10:
            rateYearResource = "refinitiv_request/10y.json";
            break;
          case 20:
            rateYearResource = "refinitiv_request/20y.json";
            break;
          case 30:
            rateYearResource = "refinitiv_request/30y.json";
            break;
          default:
            rateYearResource = "refinitiv_request/10y.json";
            break;
        }
        InputStream is = TypeReference.class.getResourceAsStream("/"+rateYearResource);
        _request = IOUtils.toString(is, "UTF-8");
      } catch (IOException e) {
        log.error(e.getMessage());
        e.printStackTrace();
      }
      connect();
    }

    /** Connect a WebSocket (and reconnect if a previous connection failed). */
    public synchronized void connect() {
      if (_websocket != null) {
        // Remove websocket from map, and create a new one based on the previous websocket.
        webSocketSessionMap.remove(this);
        try {
          _websocket = _websocket.recreate();
        } catch (IOException e) {
          log.error(e.getMessage());
          e.printStackTrace();
          // System.exit(1);
        }
      } else {
        /* Create new websocket. */
        log.info("Connecting to WebSocket " + _url + " for " + _name + "...");
        try {
          _websocket = websocketFactory.createSocket(_url).addProtocol("tr_json2").addListener(new WebSocketAdapter() {

            public void onTextMessage(WebSocket websocket, String message) throws JSONException, IOException {
              WebSocketSession webSocketSession = webSocketSessionMap.get(websocket);
              JSONArray jsonArray = new JSONArray(message);
              for (int i = 0; i < jsonArray.length(); ++i) {
                webSocketSession.processMessage(jsonArray.getJSONObject(i));
              }
            }

            public void onConnected(WebSocket websocket, Map<String, List<String>> headers) throws JSONException {
              log.info("WebSocket successfully connected for " + _name + "!");
              sendLoginRequest(true);
            }

            public void onConnectError(WebSocket websocket, WebSocketException e) {
              log.error("Connect error for " + _name + ":" + e);
              reconnect(websocket);
            }

            public void onDisconnected(WebSocket websocket, WebSocketFrame serverCloseFrame,
                WebSocketFrame clientCloseFrame, boolean closedByServer){
              log.info("WebSocket disconnected for " + _name + ".");
              // reconnect(websocket);
            }

            public void reconnect(WebSocket websocket) {
              WebSocketSession webSocketSession = webSocketSessionMap.get(websocket);
              webSocketSession.isLoggedIn(false);
              log.info("Reconnecting to " + _name + " in 3 seconds...");

              try {
                Thread.sleep(3000);
              } catch (InterruptedException e) {
                log.error(e.getMessage());
                e.printStackTrace();
                // System.exit(1);
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
      webSocketSessionMap.put(_websocket, this);
      _websocket.connectAsynchronously();
    }

    /**
     * Generate a login request from command line data (or defaults) and send Used
     * for both the initial login and subsequent logins that send updated access
     * tokens.
     * 
     * @param authToken
     * @throws JSONException
     */
    private void sendLoginRequest(boolean isFirstLogin) throws JSONException {
      String loginJsonString = "{\"ID\":1,\"Domain\":\"Login\",\"Key\":{\"Elements\":{\"ApplicationId\":\"\",\"Position\":\"\",\"AuthenticationToken\":\"\"},\"NameType\":\"AuthnToken\"}}";
      JSONObject loginJson = new JSONObject(loginJsonString);
      loginJson.getJSONObject("Key").getJSONObject("Elements").put("AuthenticationToken", _authToken);
      loginJson.getJSONObject("Key").getJSONObject("Elements").put("ApplicationId", appId);
      loginJson.getJSONObject("Key").getJSONObject("Elements").put("Position", position);

      if (!isFirstLogin) // If this isn't our first login, we don't need another refresh for it.
        loginJson.put("Refresh", false);

      _websocket.sendText(loginJson.toString());
      // log.info("SENT on " + _name + ": \n" + loginJson.toString(2));
    }

    /**
     * Create and send simple Market Price request
     * 
     * @throws JSONException
     * @throws IOException
     */
    private void sendRequest() throws JSONException, IOException {
      _websocket.sendText(_request);
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
        log.info(messageJson.toString(2));
        JSONObject currentRate = new JSONObject(messageJson.toString(2));
        String timestamp = currentRate.getJSONObject("Fields").getString("VALUE_TS1");
        double rate = currentRate.getJSONObject("Fields").getDouble("RT_YIELD_1");
        double primaryAct = currentRate.getJSONObject("Fields").getDouble("PRIMACT_1");
        String service = currentRate.getJSONObject("Key").getString("Service");
        String name = currentRate.getJSONObject("Key").getString("Name");
        Rate liveRate = new Rate(rate, primaryAct, name, service, timestamp, "uuid");
        _sink.next(liveRate);
      }
      // System.out.println(messageJson.toString(2));
      switch (messageType) {
        case "Refresh":
        case "Status":
          if (messageJson.has("Domain")) {
            String messageDomain = messageJson.getString("Domain");
            if (messageDomain.equals("Login")) {
              log.info("=> " + messageJson.toString(2));
              // Check message state to see if login succeeded. If so, send item request.
              // Otherwise stop.
              JSONObject messageState = messageJson.optJSONObject("State");
              if (messageState != null) {
                if (!messageState.getString("Stream").equals("Open") || !messageState.getString("Data").equals("Ok")) {
                  log.error("Login failed.");
                  // System.exit(1);
                }

                // Login succeeded, send item request.
                isLoggedIn(true);
                sendRequest();
              }
            }
          }
          break;

        case "Ping":
          String pongJsonString = "{\"Type\":\"Pong\"}";
          // JSONObject pongJson = new JSONObject(pongJsonString);
          _websocket.sendText(pongJsonString);
          // System.out.println("SENT on " + _name + ": \n" + pongJson.toString(2));
          // System.exit(1);
          break;
        default:
          break;
      }
    }

    /**
     * Send a login request on the websocket that includes our updated access token.
     */
    public synchronized void updateToken(String updatedAuthToken) {
      _authToken = updatedAuthToken;

      if (!_isLoggedIn)
        return; // Websocket not connected or logged in yet. Initial login will include the access token.

      // System.out.println("Refreshing the access token for " + _name);
      sendLoginRequest(false);
    }

    /**
     * Mark whether we are connected and logged in so that the updateToken method
     * knows whether or not to reissue the login.
     */
    public synchronized void isLoggedIn(boolean isLoggedIn) {
      _isLoggedIn = isLoggedIn;
    }
  }

  public static void stream(FluxSink<Rate> sink, int number, String user, String password, String clientId) {
    getLiverateRunnable runnable = new getLiverateRunnable(sink, number, user, password, clientId);
    Thread t = new Thread(runnable);
    t.start();
  }

  public static class getLiverateRunnable implements Runnable {
    FluxSink<Rate> sink;
    int number;
    String user;
    String password;
    String clientId;

    public getLiverateRunnable(FluxSink<Rate> sink, int number, String user, String password, String clientId) {
      this.sink = sink;
      this.number = number;
      this.user = user;
      this.password = password;
      this.clientId = clientId;
    }

    public void run() {
      getLiveRate(sink, number, user, password, clientId);

      // Only on complete() is the result sent to the browser
      sink.complete();
    }
  }

  public static void getLiveRate(FluxSink<Rate> sink, int number, String passedUser, String passedPassword, String passedClientId) {
    user = passedUser;
    password = passedPassword;
    clientid = passedClientId;
    try {
      position = Inet4Address.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      log.error(e.getMessage());
      // If localhost isn't defined, use 127.0.0.1.
      position = "127.0.0.1/net";
    }

    try {
      // Connect to Refinitiv Data Platform and authenticate (using our username and password)
      authJson = getAuthenticationInfo(null);
      if (authJson == null) {
        log.error("authJson is NULL");
        // System.exit(1);
      }
        

      // Get service information.
      serviceJson = queryServiceDiscovery();
      if (serviceJson == null) {
        log.error("serviceJson is NULL");
        // System.exit(1);
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
            // hostList.add(endpoint.getString("endpoint") + ":" + endpoint.getInt("port"));
            hostList.add(endpoint.getString("endpoint"));
            break;
          }
        } else {
          if (endpoint.getJSONArray("location").length() == 1)
            // hostList.add(endpoint.getString("endpoint") + ":" + endpoint.getInt("port"));
            hostList.add(endpoint.getString("endpoint"));
        }
      }

      // Determine when the access token expires. We will re-authenticate before then.
      int expireTime = Integer.parseInt(authJson.getString("expires_in"));
      // Timestamp timestamp = new Timestamp(System.currentTimeMillis());
      log.info("Year :" + number + " - Time: " + new Timestamp(System.currentTimeMillis()));   

      if (hotstandby) {
        if (hostList.size() < 2) {
          log.error("Error: Expected 2 hosts but received " + hostList.size());
          // System.exit(1);
        }
      } else {
        if (hostList.size() == 0) {
          log.error("Error: No endpoints in response.");
          // System.exit(1);
        }
      }

      // Connect WebSocket(s).
      webSocketSession1 = new WebSocketSession(sink, number, "session1", hostList.get(0), authJson.getString("access_token"));
      if (hotstandby)
        webSocketSession2 = new WebSocketSession(sink, number, "session2", hostList.get(1), authJson.getString("access_token"));

      while (true) {
        log.info("in while loop");
        // Continue using current token until 90% of initial time before it expires.
        Thread.sleep(expireTime * 900); // The value 900 means 90% of expireTime in milliseconds
        log.info(number+ " - Time 90%: " + new Timestamp(System.currentTimeMillis()));

        // Connect to Refinitiv Data Platform and re-authenticate, using the refresh
        // token provided in the previous response
        authJson = getAuthenticationInfo(authJson);
        if (authJson == null) {
          log.error("authJson is NULL");
          // System.exit(1);
        }
        log.info(number + " - auth: " + authJson.toString());

        // If expiration time returned by refresh request is less then initial
        // expiration time, re-authenticate using password
        int refreshingExpireTime = Integer.parseInt(authJson.getString("expires_in"));
        log.info(number + " - new Time: " + refreshingExpireTime);
        if (refreshingExpireTime != expireTime) {
          log.info("expire time changed from " + expireTime + " sec to " + refreshingExpireTime + " sec; retry with password");
          authJson = getAuthenticationInfo(null);
          if (authJson == null) {
            log.error("authJson is NULL");
            // System.exit(1);
          }
          expireTime = Integer.parseInt(authJson.getString("expires_in"));
        }

        // Send the updated access token over our WebSockets.
        log.info(number + " - update token");
        webSocketSession1.updateToken(authJson.getString("access_token"));
        if (hotstandby)
          webSocketSession2.updateToken(authJson.getString("access_token"));
      }
    } catch (JSONException e) {
      log.error(e.getMessage());
      e.printStackTrace();
    } catch (InterruptedException e) {
      log.error(e.getMessage());
      e.printStackTrace();
    }
  }

  /**
   * Authenticate to Refinitiv Data Platform via an HTTP post request. Initially
   * authenticates using the specified password. If information from a previous
   * authentication response is provided, it instead authenticates using the
   * refresh token from that response. Uses authUrl as url.
   * 
   * @param previousAuthResponseJson Information from a previous authentication,
   *                                 if available
   * @return A JSONObject containing the authentication information from the
   *         response.
   */
  public static JSONObject getAuthenticationInfo(JSONObject previousAuthResponseJson) {
    String url = authUrl;
    return getAuthenticationInfo(previousAuthResponseJson, url);
  }

  /**
   * Authenticate to Refinitiv Data Platform via an HTTP post request. Initially
   * authenticates using the specified password. If information from a previous
   * authentication response is provided, it instead authenticates using the
   * refresh token from that response.
   * 
   * @param previousAuthResponseJson Information from a previous authentication,
   *                                 if available
   * @param url                      The HTTP post url
   * @return A JSONObject containing the authentication information from the
   *         response.
   */
  public static JSONObject getAuthenticationInfo(JSONObject previousAuthResponseJson, String url) {
    try {
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(new SSLContextBuilder().build());
      HttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
      // HttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).disableRedirectHandling().build();
      HttpPost httppost = new HttpPost(url);
      HttpParams httpParams = new BasicHttpParams();

      // Disable redirect
      // httpParams.setParameter(ClientPNames.HANDLE_REDIRECTS, false);

      // Set request parameters.
      List<NameValuePair> params = new ArrayList<NameValuePair>(2);
      params.add(new BasicNameValuePair("client_id", clientid));
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
          log.info("Refinitiv Data Platform authentication HTTP code: "
              + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
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
          log.info("Refinitiv Data Platform authentication HTTP code: "
              + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          if (previousAuthResponseJson != null) {
            log.info("Retry with username and password");
            return getAuthenticationInfo(null);
          }
          return null;
        case HttpStatus.SC_FORBIDDEN: // 403
        case 451: // 451 Unavailable For Legal Reasons
          // Stop retrying with the request
          log.info("Refinitiv Data Platform authentication HTTP code: "
              + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          log.info("Stop retrying with the request");
          return null;
        default:
          // Retry the request to Refinitiv Data Platform
          log.info("Refinitiv Data Platform authentication HTTP code: "
              + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
          log.info("Retry the request to Refinitiv Data Platform");
          return getAuthenticationInfo(previousAuthResponseJson);
      }
    } catch (Exception e) {
      log.error("Refinitiv Data Platform authentication failure:");
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Retrive service information indicating locations to connect to. Uses
   * authHostname and authPort as url.
   * 
   * @return A JSONObject containing the service information.
   */
  public static JSONObject queryServiceDiscovery() {
    String url = discoveryUrl;
    return queryServiceDiscovery(url);
  }

  /**
   * Retrive service information indicating locations to connect to for a specific
   * host.
   * 
   * @param host the host to connect to
   * @return A JSONObject containing the service information.
   */
  public static JSONObject queryServiceDiscovery(String url) {
    try {
      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(new SSLContextBuilder().build());
      // HttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).disableRedirectHandling().build();
      HttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
      HttpGet httpget = new HttpGet(url + "?transport=websocket");
      HttpParams httpParams = new BasicHttpParams();

      // Disable redirect
      // httpParams.setParameter(ClientPNames.HANDLE_REDIRECTS, false);

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
          // System.out.println("Refinitiv Data Platform service discovery succeeded. RECEIVED:");
          // System.out.println(responseJson.toString(2));
          return responseJson;
        case HttpStatus.SC_MOVED_PERMANENTLY: // 301
        case HttpStatus.SC_MOVED_TEMPORARILY: // 302
        case HttpStatus.SC_SEE_OTHER: // 303
        case HttpStatus.SC_TEMPORARY_REDIRECT: // 307
        case 308: // 308 HttpStatus.SC_PERMANENT_REDIRECT
          // Perform URL redirect
          log.info("Refinitiv Data Platform service discovery HTTP code: "
              + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
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
          log.info("Refinitiv Data Platform service discovery HTTP code: "
              + response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
              log.info("Retry the service discovery request");
          return queryServiceDiscovery();
      }
    } catch (Exception e) {
      log.error("Refinitiv Data Platform service discovery failure:");
      e.printStackTrace();
      return null;
    }
  }
}
