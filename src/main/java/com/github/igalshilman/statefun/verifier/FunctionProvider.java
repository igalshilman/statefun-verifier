package com.github.igalshilman.statefun.verifier;

import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.StatefulFunctionProvider;

import java.util.Objects;

public class FunctionProvider implements StatefulFunctionProvider {
  private final Ids ids;

  public FunctionProvider(Ids ids) {
    this.ids = Objects.requireNonNull(ids);
  }

  @Override
  public StatefulFunction functionOfType(FunctionType functionType) {
    CommandInterpreter interpreter = new CommandInterpreter(ids);
    return new Fn(interpreter);
  }
}
