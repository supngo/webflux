package com.naturecode.webflux_stream.liverate.util;

import com.naturecode.webflux_stream.liverate.model.Emitter;

import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

@Component
@Log4j2
public class LiveRate {
  public static void stream(Emitter emitter) {
    try {
      WebSocketSession webSocketSession = WebSocketSession.getInstance();
      // log.info("Stream " + emitter.getId() + " subscribing...");
      webSocketSession.subscribe(emitter);
    } catch (Exception e) {
      log.error(e.getMessage());
      e.printStackTrace();
    }
  }

  public static void cancel(Emitter emitter) {
    WebSocketSession webSocketSession = WebSocketSession.getInstance();
    // log.info("Stream " + emitter.getId() + " unsubscribing...");
    webSocketSession.unsubscribe(emitter);
  }
}
