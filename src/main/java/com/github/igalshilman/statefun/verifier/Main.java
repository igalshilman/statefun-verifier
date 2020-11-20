package com.github.igalshilman.statefun.verifier;

import com.github.igalshilman.statefun.verifier.generated.VerificationResult;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.statefun.flink.harness.Harness;

public class Main {

  public static void main(String[] args) throws Exception {
    Harness harness = new Harness();
    // first, configure the harness with a client side flink-conf.yaml
    Configuration flinkConf = GlobalConfiguration.loadConfiguration(".");
    flinkConf.toMap().forEach(harness::withConfiguration);
    // second, override any of the configurations via the command line args.
    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    parameterTool.toMap().forEach(harness::withConfiguration);
    // when running in the IDE, you probably just want to step trough, debug the application
    // you do not need to actually verify anything.
    // uncomment the next line to print the verification results to the screen:
    // harness.withConsumingEgress(Constants.VERIFICATION_RESULT, Main::printMismatches);
    harness.start();
  }

  /**
   * Prints failed {@link VerificationResult}s.
   *
   * <p>Please note that, the first verification results are expected to fail, because of the
   * delayed and asynchronous messages that are still floating around. After few rounds of
   * verifications the printing should stop.
   */
  private static void printMismatches(VerificationResult res) {
    if (res.getExpected() == res.getActual()) {
      return;
    }
    System.out.printf(
        "%2d> expected %d, actual %d\n", res.getId(), res.getExpected(), res.getActual());
  }
}
