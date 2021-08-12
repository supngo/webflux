package com.naturecode.webflux_stream.liverate.util;

import java.util.UUID;

import com.naturecode.webflux_stream.liverate.model.Emitter;

import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

@Component
@Log4j2
public class LiveRate {
  public static void stream(Emitter emitter) {
    try {
      WebSocketSession webSocketSession = WebSocketSession.getInstance();
      webSocketSession.subscribe(emitter);
    } catch (Exception e) {
      log.error(e.getMessage());
      e.printStackTrace();
    }
  }

  public static void cancel(UUID uuid) {
    WebSocketSession webSocketSession = WebSocketSession.getInstance();
    webSocketSession.unsubscribe(uuid);
  }
}
