package com.naturecode.webflux_stream.liverate.model;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import reactor.core.publisher.FluxSink;

@Data
@AllArgsConstructor
public class Emitter {
  FluxSink<Rate> sink;
  int duration;
  UUID id;
;}
