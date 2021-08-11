package com.naturecode.webflux_stream.liverate.model;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Comment {
  private String author;
  private String message;
  private String timestamp;
}