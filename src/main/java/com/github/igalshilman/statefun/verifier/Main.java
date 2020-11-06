package com.github.igalshilman.statefun.verifier;

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
    harness.start();
  }
}
