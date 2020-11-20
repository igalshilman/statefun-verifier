package com.github.igalshilman.statefun.verifier.e2e;

import com.github.igalshilman.statefun.verifier.Main;
import com.github.igalshilman.statefun.verifier.ModuleParameters;
import com.github.igalshilman.statefun.verifier.generated.VerificationResult;
import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.github.igalshilman.statefun.verifier.e2e.Constants.GLOBAL_CONFIG_PREFIX;

public class VerifierEndToEndTest {

  @Test(timeout = 1_000 * 60 * 2)
  public void run() {
    List<String> containerArgs = new ArrayList<>();

    ModuleParameters parameters = new ModuleParameters();
    parameters.setNumberOfFunctionInstances(128);
    parameters.setMessageCount(100_000);
    parameters.setMaxFailures(1);

    parameters
        .asMap()
        .forEach(
            (key, val) -> {
              containerArgs.add(GLOBAL_CONFIG_PREFIX + key);
              containerArgs.add(val);
            });

    SimpleTcpServer server = new SimpleTcpServer();
    SimpleTcpServer.StartedServer bound = server.start();

    containerArgs.add(GLOBAL_CONFIG_PREFIX + Constants.PORT_KEY);
    containerArgs.add("" + bound.port());
    containerArgs.add(GLOBAL_CONFIG_PREFIX + Constants.HOST_KEY);
    containerArgs.add("localhost");

    CompletableFuture.runAsync(
        () -> {
          try {
            ClassLoader cl = new URLClassLoader(new URL[0], Main.class.getClassLoader());
            Thread.currentThread().setContextClassLoader(cl);
            Main.main(containerArgs.toArray(new String[0]));
          } catch (Exception e) {
            e.printStackTrace();
          }
        });

    Supplier<VerificationResult> results = bound.results();
    Set<Integer> set = new HashSet<>();
    while (set.size() != parameters.getNumberOfFunctionInstances()) {
      VerificationResult result = results.get();
      if (result.getActual() == result.getExpected()) {
        set.add(result.getId());
      }
    }
    System.out.println("done");
  }
}
