package com.github.igalshilman.statefun.verifier;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ModuleParametersTest {

  @Test
  public void exampleUsage() {
    Map<String, String> keys = Collections.singletonMap("messageCount", "1");
    ModuleParameters parameters = ModuleParameters.from(keys);

    assertThat(parameters.getMessageCount(), is(1));
  }
}
