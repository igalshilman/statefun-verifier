package com.github.igalshilman.statefun.verifier;

import org.junit.Test;

import static com.github.igalshilman.statefun.verifier.Utils.aRelayedStateModificationCommand;
import static com.github.igalshilman.statefun.verifier.Utils.aStateModificationCommand;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class FunctionStateTrackerTest {

  @Test
  public void exampleUsage() {
    FunctionStateTracker tracker = new FunctionStateTracker(1_000);

    tracker.apply(aStateModificationCommand(5));
    tracker.apply(aStateModificationCommand(5));
    tracker.apply(aStateModificationCommand(5));

    assertThat(tracker.stateOf(5), is(3L));
  }

  @Test
  public void testRelay() {
    FunctionStateTracker tracker = new FunctionStateTracker(1_000);

    // send a layered state increment message, first to function 5, and then
    // to function 6.
    tracker.apply(aRelayedStateModificationCommand(5, 6));

    assertThat(tracker.stateOf(5), is(0L));
    assertThat(tracker.stateOf(6), is(1L));
  }
}
