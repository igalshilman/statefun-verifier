package com.github.igalshilman.statefun.verifier;

import com.github.igalshilman.statefun.verifier.generated.Command;
import com.github.igalshilman.statefun.verifier.generated.Commands;
import com.github.igalshilman.statefun.verifier.generated.FnAddress;
import com.github.igalshilman.statefun.verifier.generated.FunctionTrackerSnapshot;
import com.github.igalshilman.statefun.verifier.generated.SourceCommand;
import com.google.protobuf.InvalidProtocolBufferException;

public final class FunctionStateTracker {
  private final long[] expectedStates;

  public FunctionStateTracker(int numberOfFunctionInstances) {
    this.expectedStates = new long[numberOfFunctionInstances];
  }

  /**
   * Find any state modification commands nested under @sourceCommand, and apply them in the
   * internal state representation.
   */
  public void apply(SourceCommand sourceCommand) {
    updateInternally(sourceCommand.getTarget(), sourceCommand.getCommands());
  }

  /** Apply all the state modification stored in the snapshot represented by the snapshotBytes. */
  public FunctionStateTracker apply(byte[] snapshotBytes) throws InvalidProtocolBufferException {
    FunctionTrackerSnapshot snapshot = FunctionTrackerSnapshot.parseFrom(snapshotBytes);
    for (int i = 0; i < snapshot.getStateCount(); i++) {
      expectedStates[i] += snapshot.getState(i);
    }
    return this;
  }

  /** Get the current expected state of a function instance. */
  public long stateOf(int id) {
    return expectedStates[id];
  }

  public byte[] snapshot() {
    FunctionTrackerSnapshot.Builder snapshot = FunctionTrackerSnapshot.newBuilder();
    for (long state : expectedStates) {
      snapshot.addState(state);
    }
    return snapshot.build().toByteArray();
  }

  /**
   * Recursively traverse the commands tree and look for {@link
   * com.github.igalshilman.statefun.verifier.generated.Command.ModifyState} commands. For each
   * {@code ModifyState} command found update the corresponding expected state.
   */
  private void updateInternally(FnAddress currentAddress, Commands commands) {
    for (Command command : commands.getCommandList()) {
      if (command.hasModify()) {
        long delta = command.getModify().getDelta();
        expectedStates[currentAddress.getId()] += delta;
      } else if (command.hasSend()) {
        updateInternally(command.getSend().getTarget(), command.getSend().getCommands());
      } else if (command.hasSendAfter()) {
        updateInternally(command.getSendAfter().getTarget(), command.getSendAfter().getCommands());
      } else if (command.hasAsyncOperation()) {
        updateInternally(currentAddress, command.getAsyncOperation().getResolvedCommands());
      }
    }
  }
}
