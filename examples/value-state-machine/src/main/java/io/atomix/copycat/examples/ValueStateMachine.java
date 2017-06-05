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
package io.atomix.copycat.examples;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

/**
 * Value state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ValueStateMachine extends StateMachine {
  private Object value;

  @Override
  public void snapshot(SnapshotWriter writer) {

  }

  @Override
  public void install(SnapshotReader reader) {

  }

  @Override
  protected void configure(StateMachineExecutor executor) {
    executor.register(SetCommand.class, this::set);
    executor.register(GetQuery.class, this::get);
    executor.register(DeleteCommand.class, this::delete);
  }

  /**
   * Sets the value.
   */
  private Object set(Commit<SetCommand> commit) {
    Object previous = value;
    value = commit;
    return previous;
  }

  /**
   * Gets the value.
   */
  private Object get(Commit<GetQuery> commit) {
    return value;
  }

  /**
   * Deletes the value.
   */
  private void delete(Commit<DeleteCommand> commit) {
    value = null;
  }

}
