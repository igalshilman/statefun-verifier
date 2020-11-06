package com.github.igalshilman.statefun.verifier;

import com.github.igalshilman.statefun.verifier.generated.FnAddress;
import com.github.igalshilman.statefun.verifier.generated.SourceCommand;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.io.Router;

import java.util.Objects;

public class CommandRouter implements Router<SourceCommand> {
  private final Ids ids;

  public CommandRouter(Ids ids) {
    this.ids = Objects.requireNonNull(ids);
  }

  @Override
  public void route(SourceCommand sourceCommand, Downstream<SourceCommand> downstream) {
    FnAddress target = sourceCommand.getTarget();
    FunctionType type = Constants.FN_TYPE;
    String id = ids.idOf(target);
    downstream.forward(type, id, sourceCommand);
  }
}
