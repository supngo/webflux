package com.naturecode.webflux_stream.liverate.repo;

import com.naturecode.webflux_stream.liverate.model.Comment;

import reactor.core.publisher.Flux;

public interface CommentRepo {

  Flux<Comment> findAll();

}