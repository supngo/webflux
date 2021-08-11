package com.naturecode.webflux_stream.liverate.repo;

import java.time.LocalDateTime;
import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;

import com.naturecode.webflux_stream.liverate.model.Rate;

import org.springframework.stereotype.Repository;

import reactor.core.publisher.Flux;

@Repository
public class ReactiveRateRepo implements RateRepo {
  private static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
  @Override
  public Flux<Rate> consume() {
    // simulate data streaming every 1 second.
    return Flux.interval(Duration.ofSeconds(1))
    .onBackpressureDrop()
    .map(this::consumeRate)
    .flatMapIterable(x -> x);
  }

  private List<Rate> consumeRate(long interval) {
    Rate obj = new Rate(1.235, 103.12, "US10YT=RRPS", "ELEKTRON_DD", dtf.format(LocalDateTime.now()));
    return Arrays.asList(obj);
  }
}
