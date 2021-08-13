package com.naturecode.webflux_stream.liverate.controller;

import java.util.UUID;

import com.naturecode.webflux_stream.liverate.model.Emitter;
import com.naturecode.webflux_stream.liverate.model.Rate;
import com.naturecode.webflux_stream.liverate.repo.RateRepo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.log4j.Log4j2;

import com.naturecode.webflux_stream.liverate.util.LiveRate;
import com.naturecode.webflux_stream.liverate.util.SlowCounter;
import com.naturecode.webflux_stream.liverate.util.WebSocketSession;

import reactor.core.publisher.Flux;

@RestController
@Log4j2
public class RateController {
  @Autowired
  private RateRepo rateRepo;

  @Value("${refinitiv.user}")
  private String user;

  @Value("${refinitiv.password}")
  private String password;

  @Value("${refinitiv.clientid}")
  private String clientId;

  @GetMapping(path = "/rate/consume", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Rate> feed() {
    return this.rateRepo.consume();
  }

  @GetMapping(path = "/rate/count/{number}", produces=MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Integer> streamCountToNumber(@PathVariable("number") int number) {
    Flux<Integer> dynamicFlux = Flux.create(sink -> {
      SlowCounter.count(sink, number);
    });
    return dynamicFlux;
  }

  @CrossOrigin
  @GetMapping(path = "/rate/stream/{number}", produces=MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Rate> streamLiveRate(@PathVariable("number") int number) {
    log.info("calling LiveRate...");
    WebSocketSession.runInstance(user, password, clientId);

    Flux<Rate> dynamicFlux = Flux.create(sink -> {
      UUID uuid = UUID.randomUUID();
      Emitter emitter = new Emitter(sink, number, uuid);
      log.info("Stream " + uuid.toString() + " starting...");
      Rate head = new Rate(0.0, 0.0, "", "", "", uuid.toString());
      sink.next(head);
      LiveRate.stream(emitter);

      sink.onDispose(() -> {
        log.info("Stream " + uuid.toString() + " disposing...");
        LiveRate.cancel(uuid);
      });
    });
    return dynamicFlux;
  }

  @CrossOrigin
  @GetMapping(path = "/rate/stream/cancel/{uuid}", produces=MediaType.TEXT_EVENT_STREAM_VALUE)
  public ResponseEntity<Object> cancelLiveRate(@PathVariable("uuid") String uuid) {
    log.info("cancelling LiveRate...");
    try {
      UUID listener = UUID.fromString(uuid);
      LiveRate.cancel(listener);
      return ResponseEntity.ok().body("");
    } catch (Exception e) {
      log.error(e.getMessage());
      e.printStackTrace();
      return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Server Error");
    }
  }
}
