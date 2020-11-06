package com.github.igalshilman.statefun.verifier;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class FunctionProvider implements StatefulFunctionProvider {
  private final ScheduledExecutorService asyncExecutorService;
  private final Ids ids;

  public FunctionProvider(Ids ids, ScheduledExecutorService asyncExecutorService) {
    this.ids = Objects.requireNonNull(ids);
      this.asyncExecutorService = asyncExecutorService;
  }



  @Override
  public StatefulFunction functionOfType(FunctionType functionType) {
    CommandInterpreter interpreter = new CommandInterpreter(ids, asyncExecutorService);
    return new Fn(interpreter);
  }
}
