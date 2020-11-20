package com.github.igalshilman.statefun.verifier.e2e;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class MoreExecutors {

  static ExecutorService newCachedDaemonThreadPool() {
    return Executors.newCachedThreadPool(
        r -> {
          Thread t = new Thread(r);
          t.setDaemon(true);
          return t;
        });
  }
}
