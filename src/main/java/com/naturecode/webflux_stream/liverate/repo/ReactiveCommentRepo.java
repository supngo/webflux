package com.naturecode.webflux_stream.liverate.repo;

import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import com.naturecode.webflux_stream.liverate.model.Comment;
import com.naturecode.webflux_stream.liverate.util.CommentGenerator;

@Repository
public class ReactiveCommentRepo implements CommentRepo {

  @Override
  public Flux<Comment> findAll() {
    // simulate data streaming every 1 second.
    return Flux.interval(Duration.ofSeconds(1))
    .onBackpressureDrop()
    .map(this::generateComment)
    .flatMapIterable(x -> x);
  }

  private List<Comment> generateComment(long interval) {
    Comment obj = new Comment(CommentGenerator.randomAuthor(), CommentGenerator.randomMessage(),
        CommentGenerator.getCurrentTimeStamp());
    return Arrays.asList(obj);
  }
}