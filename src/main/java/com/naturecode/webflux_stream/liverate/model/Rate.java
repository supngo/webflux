package com.naturecode.webflux_stream.liverate.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Rate {
  private double rate;
  private double primaryAct;
  private String name;
  private String service;
  private String timestamp;
}
