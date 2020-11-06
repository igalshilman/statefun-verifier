package com.github.igalshilman.statefun.verifier;

import com.github.igalshilman.statefun.verifier.generated.Command;
import com.github.igalshilman.statefun.verifier.generated.Commands;
import com.github.igalshilman.statefun.verifier.generated.FnAddress;
import com.github.igalshilman.statefun.verifier.generated.SourceCommand;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class CommandFlinkSource extends RichSourceFunction<SourceCommand>
    implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(CommandFlinkSource.class);

  // ------------------------------------------------------------------------------------------------------------
  // State Descriptors
  // ------------------------------------------------------------------------------------------------------------

  private static final ListStateDescriptor<Integer> COMMANDS_SENT_SO_FAR_DESCRIPTOR =
      new ListStateDescriptor<>("messages_sent", BasicTypeInfo.INT_TYPE_INFO);

  private static final ListStateDescriptor<byte[]> FUNCTION_STATE_TRACKER_DESCRIPTOR =
      new ListStateDescriptor<>("states", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

  // ------------------------------------------------------------------------------------------------------------
  // Configuration
  // ------------------------------------------------------------------------------------------------------------

  private final ModuleParameters moduleParameters;

  // ------------------------------------------------------------------------------------------------------------
  // Runtime
  // ------------------------------------------------------------------------------------------------------------

  private transient ListState<Integer> commandsSentSoFarHandle;
  private transient int commandsSentSoFar;

  private transient ListState<byte[]> functionStateTrackerHandle;
  private transient FunctionStateTracker functionStateTracker;

  private transient boolean isRestored;
  private volatile boolean done;

  public CommandFlinkSource(ModuleParameters moduleParameters) {
    this.moduleParameters = Objects.requireNonNull(moduleParameters);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    OperatorStateStore store = context.getOperatorStateStore();
    functionStateTrackerHandle = store.getUnionListState(FUNCTION_STATE_TRACKER_DESCRIPTOR);
    commandsSentSoFarHandle = store.getUnionListState(COMMANDS_SENT_SO_FAR_DESCRIPTOR);
    isRestored = context.isRestored();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    byte[] bytes = getOnlyElement(functionStateTrackerHandle.get(), new byte[0]);
    functionStateTracker =
        new FunctionStateTracker(moduleParameters.getNumberOfFunctionInstances()).apply(bytes);
    commandsSentSoFar = getOnlyElement(commandsSentSoFarHandle.get(), 0);
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    functionStateTrackerHandle.clear();
    functionStateTrackerHandle.add(functionStateTracker.snapshot());
    commandsSentSoFarHandle.clear();
    commandsSentSoFarHandle.add(commandsSentSoFar);

    double perCent = 100.0d * commandsSentSoFar / moduleParameters.getMessageCount();
    LOG.info(
        "Commands sent {} / {} ({} %)",
        commandsSentSoFar, moduleParameters.getMessageCount(), perCent);
  }

  @Override
  public void run(SourceContext<SourceCommand> ctx) {
    generate(ctx);
    if (done) {
      return;
    }
    LOG.info(
        "Generation phase complete. Will wait for {} ms",
        moduleParameters.getSleepTimeBeforeVerifyMs());
    sleep(moduleParameters.getSleepTimeBeforeVerifyMs());
    LOG.info("Starting verification phase.");
    verify(ctx);
    LOG.info(
        "All verification messages sent, about to wait for {} ms",
        moduleParameters.getSleepTimeAfterVerifyMs());
    sleep(moduleParameters.getSleepTimeAfterVerifyMs());
    LOG.info("Verification completed successfully.");
  }

  private void generate(SourceContext<SourceCommand> ctx) {
    final int startPosition = this.commandsSentSoFar;
    final int kaboomIndex = computeFailureIndex(startPosition, isRestored).orElse(-1);
    LOG.info(
        "starting at {}, kaboom at {}, total messages {}",
        startPosition,
        kaboomIndex,
        moduleParameters.getMessageCount());

    Supplier<SourceCommand> generator =
        new CommandGenerator(new JDKRandomGenerator(), moduleParameters);
    FunctionStateTracker functionStateTracker = this.functionStateTracker;
    for (int i = startPosition; i < moduleParameters.getMessageCount(); i++) {
      if (done) {
        return;
      }
      if (i == kaboomIndex) {
        LOG.info("KABOOM at message {}", i);
        throw new RuntimeException("KABOOM!!!");
      }
      SourceCommand command = generator.get();
      synchronized (ctx.getCheckpointLock()) {
        functionStateTracker.apply(command);
        ctx.collect(command);
        this.commandsSentSoFar = i;
      }
    }
  }

  private void verify(SourceContext<SourceCommand> ctx) {
    FunctionStateTracker functionStateTracker = this.functionStateTracker;

    for (int i = 0; i < moduleParameters.getNumberOfFunctionInstances(); i++) {
      final long expected = functionStateTracker.stateOf(i);

      Command.Builder verify =
          Command.newBuilder().setVerify(Command.Verify.newBuilder().setExpected(expected));

      SourceCommand command =
          SourceCommand.newBuilder()
              .setTarget(FnAddress.newBuilder().setType(0).setId(i))
              .setCommands(Commands.newBuilder().addCommand(verify))
              .build();
      synchronized (ctx.getCheckpointLock()) {
        ctx.collect(command);
      }
    }
  }

  @Override
  public void cancel() {
    done = true;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Utils
  // ---------------------------------------------------------------------------------------------------------------

  private OptionalInt computeFailureIndex(int startPosition, boolean isRestored) {
    if (isRestored) {
      return OptionalInt.empty();
    }
    if (startPosition >= moduleParameters.getMessageCount()) {
      return OptionalInt.empty();
    }
    int index =
        ThreadLocalRandom.current().nextInt(startPosition, moduleParameters.getMessageCount());
    return OptionalInt.of(index);
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T getOnlyElement(Iterable<T> items, T def) {
    Iterator<T> it = items.iterator();
    if (!it.hasNext()) {
      return def;
    }
    T item = it.next();
    if (it.hasNext()) {
      throw new IllegalStateException("Iterable has additional elements");
    }
    return item;
  }
}
