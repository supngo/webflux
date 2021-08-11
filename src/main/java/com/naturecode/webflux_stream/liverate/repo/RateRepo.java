package com.naturecode.webflux_stream.liverate.repo;

import com.naturecode.webflux_stream.liverate.model.Rate;

import reactor.core.publisher.Flux;

public interface RateRepo {
  Flux<Rate> consume();
}