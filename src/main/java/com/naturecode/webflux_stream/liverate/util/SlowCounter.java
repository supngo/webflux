package com.naturecode.webflux_stream.liverate.util;

import java.util.Random;

import reactor.core.publisher.FluxSink;

public class SlowCounter {

  private SlowCounter() {}

  public static void count(FluxSink<Integer> sink, int number) {
    SlowCounterRunnable runnable = new SlowCounterRunnable(sink, number);
    Thread t = new Thread(runnable);
    t.start();
  }

  public static class SlowCounterRunnable implements Runnable {
    FluxSink<Integer> sink;
    int number;
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    public SlowCounterRunnable(FluxSink<Integer> sink, int number) {
      this.sink = sink;
      this.number = number;
    }

    public void run() {
      int count = 0;
      while (count < number) {
        try {
          int sleep = RANDOM.nextInt(5) * 1000;
          System.out.println("sleeping for: " + sleep + " seconds");
          Thread.sleep(sleep);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        sink.next(count);
        count++;
      }
      // Only on complete() is the result sent to the browser
      sink.complete();
    }
  }
}