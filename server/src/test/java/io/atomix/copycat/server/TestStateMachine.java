package io.atomix.copycat.server;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;

/**
 * Test state machine.
 */
public class TestStateMachine extends StateMachine {
  /**
   * Test command.
   */
  public static class TestCommand implements Command<String> {
    public String value;

    public TestCommand(String value) {
      this.value = value;
    }
  }

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(TestCommand.class, this::command);
  }

  private String command(Commit<TestCommand> commit) {
    return commit.operation().value;
  }

  /**
   * Test query.
   */
  public static class TestQuery implements Query<String> {
  }

}