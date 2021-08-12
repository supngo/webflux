package com.naturecode.webflux_stream.liverate.model;

import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import reactor.core.publisher.FluxSink;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Emitter {
  FluxSink<Rate> sink;
  int duration;
  UUID id;
;}
