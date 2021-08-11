package com.naturecode.webflux_stream.liverate;

import java.io.InputStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.naturecode.webflux_stream.liverate.util.WebSocketSession;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

@Component
@Log4j2
public class RunWebSocket implements CommandLineRunner {
  @Value("${refinitiv.user}")
  private String user;

  @Value("${refinitiv.password}")
  private String password;

  @Value("${refinitiv.clientid}")
  private String clientId;
  
  @Override
  public void run(String... args) throws Exception {
    log.info("WebSocket connecting...");
    WebSocketSession.runInstance(user, password, clientId);
    
    String requestJson = "/refinitiv_request/all.json";
    InputStream is = TypeReference.class.getResourceAsStream(requestJson);
    log.info("Start streaming in 8 seconds...");
    Thread.sleep(8000);
    WebSocketSession webSocketSession = WebSocketSession.getInstance();
    webSocketSession.sendRequest(IOUtils.toString(is, "UTF-8"));
  }
}
