package com.github.igalshilman.statefun.verifier;

import com.github.igalshilman.statefun.verifier.generated.SourceCommand;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class CommandGeneratorTest {

  @Test
  public void usageExample() {
    ModuleParameters parameters = new ModuleParameters();
    CommandGenerator generator = new CommandGenerator(new JDKRandomGenerator(), parameters);

    SourceCommand command = generator.get();

    assertThat(command.getTarget(), notNullValue());
    assertThat(command.getCommands(), notNullValue());
  }
}
