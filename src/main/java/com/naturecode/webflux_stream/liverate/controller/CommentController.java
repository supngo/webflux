package com.naturecode.webflux_stream.liverate.controller;

import com.naturecode.webflux_stream.liverate.model.Comment;
import com.naturecode.webflux_stream.liverate.repo.CommentRepo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class CommentController {

  @Autowired
  private CommentRepo commentRepo;

  @GetMapping(path = "/comment/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public Flux<Comment> feed() {
    return this.commentRepo.findAll();
  }
}