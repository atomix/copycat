/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.copycat.server;

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;

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