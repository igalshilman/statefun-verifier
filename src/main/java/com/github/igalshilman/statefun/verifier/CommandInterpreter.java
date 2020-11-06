package com.github.igalshilman.statefun.verifier;

import com.github.igalshilman.statefun.verifier.Constants;
import com.github.igalshilman.statefun.verifier.Ids;
import com.github.igalshilman.statefun.verifier.generated.Command;
import com.github.igalshilman.statefun.verifier.generated.Commands;
import com.github.igalshilman.statefun.verifier.generated.SourceCommand;
import com.google.protobuf.Any;
import org.apache.flink.statefun.sdk.AsyncOperationResult;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.state.PersistedValue;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class CommandInterpreter {
  private static final Throwable ASYNC_OP_THROWABLE;

  static {
    ASYNC_OP_THROWABLE = new RuntimeException();
    ASYNC_OP_THROWABLE.setStackTrace(new StackTraceElement[] {});
  }

  private final ScheduledExecutorService service;
  private final Ids ids;

  public CommandInterpreter(Ids ids, ScheduledExecutorService service) {
    this.service = service;
    this.ids = Objects.requireNonNull(ids);
  }

  public void interpret(PersistedValue<Long> state, Context context, Object message) {
    if (message instanceof SourceCommand) {
      Commands sourceCommand = ((SourceCommand) message).getCommands();
      interpret(state, context, sourceCommand);
    } else if (message instanceof Commands) {
      interpret(state, context, (Commands) message);
    } else if (message instanceof AsyncOperationResult) {
      @SuppressWarnings("unchecked")
      AsyncOperationResult<Commands, ?> res = (AsyncOperationResult<Commands, ?>) message;
      interpret(state, context, res.metadata());
    } else {
      throw new IllegalStateException("wtf " + message);
    }
  }

  private void interpret(PersistedValue<Long> state, Context context, Commands command) {
    for (Command cmd : command.getCommandList()) {
      if (cmd.hasModify()) {
        modifyState(state, context, cmd.getModify());
      } else if (cmd.hasAsyncOperation()) {
        registerAsyncOps(state, context, cmd.getAsyncOperation());
      } else if (cmd.hasSend()) {
        send(state, context, cmd.getSend());
      } else if (cmd.hasSendAfter()) {
        sendAfter(state, context, cmd.getSendAfter());
      } else if (cmd.hasSendEgress()) {
        sendEgress(state, context, cmd.getSendEgress());
      } else if (cmd.hasVerify()) {
        verify(state, context, cmd.getVerify());
      }
    }
  }

  private void verify(
      PersistedValue<Long> state,
      @SuppressWarnings("unused") Context context,
      Command.Verify verify) {
    long actual = state.getOrDefault(0L);
    long expected = verify.getExpected();
    if (expected != actual) {
      throw new IllegalStateException("Verification failed " + expected + " vs " + actual);
    }
  }

  private void sendEgress(
      @SuppressWarnings("unused") PersistedValue<Long> state,
      Context context,
      @SuppressWarnings("unused") Command.SendEgress sendEgress) {
    context.send(Constants.OUT, Any.getDefaultInstance());
  }

  private void sendAfter(
      @SuppressWarnings("unused") PersistedValue<Long> state,
      Context context,
      Command.SendAfter send) {
    FunctionType functionType = Constants.FN_TYPE;
    String id = ids.idOf(send.getTarget());
    Duration delay = Duration.ofMillis(send.getDurationMs());
    context.sendAfter(delay, functionType, id, send.getCommands());
  }

  private void send(
      @SuppressWarnings("unused") PersistedValue<Long> state, Context context, Command.Send send) {
    FunctionType functionType = Constants.FN_TYPE;
    String id = ids.idOf(send.getTarget());
    context.send(functionType, id, send.getCommands());
  }

  private void registerAsyncOps(
      @SuppressWarnings("unused") PersistedValue<Long> state,
      Context context,
      Command.AsyncOperation asyncOperation) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    boolean failure = asyncOperation.getFailure();
    service.schedule(
        () -> {
          if (failure) {
            future.completeExceptionally(ASYNC_OP_THROWABLE);
          } else {
            future.complete(true);
          }
        },
        asyncOperation.getResolveAfterMs(),
        TimeUnit.MILLISECONDS);

    Commands next = asyncOperation.getResolvedCommands();
    context.registerAsyncOperation(next, future);
  }

  private void modifyState(
      PersistedValue<Long> state,
      @SuppressWarnings("unused") Context context,
      Command.ModifyState modifyState) {
    state.updateAndGet(n -> n == null ? modifyState.getDelta() : n + modifyState.getDelta());
  }
}
